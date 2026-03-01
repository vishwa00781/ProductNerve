from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys
import os

# Creation of Spark Session
spark = SparkSession.builder \
    .appName("GoldProcessing") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
    .getOrCreate()

bucket = sys.argv[1]

print("Starting gold processing")

# 1. Reading all the tables from Silver Layer 
silver_user = spark.read.csv(f"s3a://{bucket}/silver/dim_user/*.csv", header=True, inferSchema=True)
silver_device = spark.read.csv(f"s3a://{bucket}/silver/dim_device/*.csv", header=True, inferSchema=True)
silver_geo = spark.read.csv(f"s3a://{bucket}/silver/dim_geo/*.csv", header=True, inferSchema=True)
silver_session = spark.read.csv(f"s3a://{bucket}/silver/dim_session/*.csv", header=True, inferSchema=True)
silver_page = spark.read.csv(f"s3a://{bucket}/silver/dim_page/*.csv", header=True, inferSchema=True)
silver_fact = spark.read.csv(f"s3a://{bucket}/silver/fact_events/*.csv", header=True, inferSchema=True)

print(" Loaded all silver tables")

# 2. Loading the tables into Gold Schema

gold_dim_user = (
    silver_user
    .withColumn("user_id", F.coalesce(F.col("user_id"), F.col("distinct_id")))
    .withColumn("user_key", F.upper("user_key"))
    .select(
        F.col("user_key").cast(StringType()).alias("USER_KEY"),
        F.col("distinct_id").cast(StringType()).alias("DISTINCT_ID"),
        F.col("person_id").cast(StringType()).alias("PERSON_ID"),
        F.col("user_id").cast(StringType()).alias("USER_ID"),
    )
)

gold_dim_device = (
    silver_device
    .withColumn("device_key", F.upper("device_key"))
    .select(
        F.col("device_key").cast(StringType()).alias("DEVICE_KEY"),
        F.col("browser").cast(StringType()).alias("BROWSER"),
        F.col("os").cast(StringType()).alias("OS"),
    )
)

gold_dim_geo = (
    silver_geo
    .withColumn("geo_key", F.upper("geo_key"))
    .select(
        F.col("geo_key").cast(StringType()).alias("GEO_KEY"),
        F.col("country_code").cast(StringType()).alias("COUNTRY_CODE"),
    )
)

gold_dim_session = (
    silver_session
    .withColumn("session_key", F.upper("session_key"))
    .select(
        F.col("session_key").cast(StringType()).alias("SESSION_KEY"),
        F.col("session_id").cast(StringType()).alias("SESSION_ID"),
    )
)

gold_dim_page = (
    silver_page
    .withColumn("page_key", F.upper("page_key"))
    .select(
        F.col("page_key").cast(StringType()).alias("PAGE_KEY"),
        F.col("pathname").cast(StringType()).alias("PATHNAME"),
    )
)

gold_fact_events = (
    silver_fact
    .withColumn("user_key", F.upper("user_key"))
    .withColumn("device_key", F.upper("device_key"))
    .withColumn("geo_key", F.upper("geo_key"))
    .withColumn("session_key", F.upper("session_key"))
    .withColumn("page_key", F.upper("page_key"))
    .select(
        F.col("event_uuid").cast(StringType()).alias("EVENT_UUID"),
        F.col("event_name").cast(StringType()).alias("EVENT_NAME"),
        F.col("event_timestamp").cast(TimestampType()).alias("EVENT_TIMESTAMP"),
        F.col("user_key").cast(StringType()).alias("USER_KEY"),
        F.col("device_key").cast(StringType()).alias("DEVICE_KEY"),
        F.col("geo_key").cast(StringType()).alias("GEO_KEY"),
        F.col("session_key").cast(StringType()).alias("SESSION_KEY"),
        F.col("page_key").cast(StringType()).alias("PAGE_KEY"),
    )
)

# 3. Writing the tables in S3 Gold
GOLD_TABLES = {
    "dim_user": gold_dim_user,
    "dim_device": gold_dim_device,
    "dim_geo": gold_dim_geo,
    "dim_session": gold_dim_session,
    "dim_page": gold_dim_page,
    "fact_events": gold_fact_events,
}

for table_name, df in GOLD_TABLES.items():
    output_path = f"s3a://{bucket}/gold/{table_name}/"
    df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(output_path)
    print(f"✓ {table_name} → {output_path}")

print(f" Gold layer complete - {gold_fact_events.count()} events processed")

# 4. Loading the tables into Snowflake
print("Starting Snowflake load...")

import snowflake.connector
import os

#  Snowflake Connection 
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    database="POSTHOG_ANALYTICS",
    schema="GOLD",
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role="ACCOUNTADMIN"
)

cursor = conn.cursor()

database = "POSTHOG_ANALYTICS"
schema = "GOLD"

# 1. Creation of Tables If does not Exists 
print("Ensuring tables exist...")

TABLE_DDLS = {
    "dim_user": f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.dim_user (
            USER_KEY STRING,
            DISTINCT_ID STRING,
            PERSON_ID STRING,
            USER_ID STRING
        );
    """,

    "dim_device": f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.dim_device (
            DEVICE_KEY STRING,
            BROWSER STRING,
            OS STRING
        );
    """,

    "dim_geo": f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.dim_geo (
            GEO_KEY STRING,
            COUNTRY_CODE STRING
        );
    """,

    "dim_session": f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.dim_session (
            SESSION_KEY STRING,
            SESSION_ID STRING
        );
    """,

    "dim_page": f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.dim_page (
            PAGE_KEY STRING,
            PATHNAME STRING
        );
    """,

    "fact_events": f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.fact_events (
            EVENT_UUID STRING,
            EVENT_NAME STRING,
            EVENT_TIMESTAMP TIMESTAMP,
            USER_KEY STRING,
            DEVICE_KEY STRING,
            GEO_KEY STRING,
            SESSION_KEY STRING,
            PAGE_KEY STRING
        );
    """
}

for table, ddl in TABLE_DDLS.items():
    cursor.execute(ddl)
    print(f" Table ready: {table}")

# 2. Truncate & Load Dimensions 
for table in ['dim_user', 'dim_device', 'dim_geo', 'dim_session', 'dim_page']:
    print(f"Loading {table}...")

    cursor.execute(f"TRUNCATE TABLE {database}.{schema}.{table};")


    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    cursor.execute(f"""
        COPY INTO {database}.{schema}.{table}
        FROM 's3://rawdataingestiontest1/gold/{table}/'
        CREDENTIALS = (
        AWS_KEY_ID = '{aws_key}'
        AWS_SECRET_KEY ='{aws_secret}'
           )
        FILE_FORMAT = ( TYPE = PARQUET )
        PATTERN = '.*\\.parquet$'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = TRUE;
        """)

    
    print(f" Loaded {table}")

# 3. Truncate & Load Fact Table
print("Loading fact_events...")

cursor.execute(f"TRUNCATE TABLE {database}.{schema}.fact_events;")

cursor.execute(f"""
    COPY INTO {database}.{schema}.fact_events
    FROM 's3://rawdataingestiontest1/gold/fact_events/'
    CREDENTIALS = (
        AWS_KEY_ID = '{aws_key}'
        AWS_SECRET_KEY ='{aws_secret}'
    )
    FILE_FORMAT = ( TYPE = PARQUET )
    PATTERN = '.*\\.parquet$'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    PURGE = TRUE;
""")

print("Loaded fact_events")

#  4. Validation of Row Counts 
print("Validating row counts...")

cursor.execute(f"""
    SELECT 'dim_user' AS table_name, COUNT(*) FROM {database}.{schema}.dim_user
    UNION ALL SELECT 'dim_device', COUNT(*) FROM {database}.{schema}.dim_device
    UNION ALL SELECT 'dim_geo', COUNT(*) FROM {database}.{schema}.dim_geo
    UNION ALL SELECT 'dim_session', COUNT(*) FROM {database}.{schema}.dim_session
    UNION ALL SELECT 'dim_page', COUNT(*) FROM {database}.{schema}.dim_page
    UNION ALL SELECT 'fact_events', COUNT(*) FROM {database}.{schema}.fact_events;
""")

for row in cursor.fetchall():
    print(f" {row[0]}: {row[1]:,} rows")

cursor.close()
conn.close()

print(" Snowflake load complete")
