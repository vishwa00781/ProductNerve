#!/usr/bin/env python3
"""
Silver Layer Processing - Spark Job
Runs on Spark cluster, processes bronze → silver
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Spark Session

spark = (
    SparkSession.builder
    .appName("SilverProcessing")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
    .getOrCreate()
)


# Arguments

bucket = sys.argv[1]
execution_date = sys.argv[2]

print(f" Starting Silver Processing for date: {execution_date}")


# 1. Reading of Bronze Layer

bronze_path = f"s3a://{bucket}/raw/Database.csv"
raw = spark.read.option("header", True).option("inferSchema", True).csv(bronze_path)

print("Rows in bronze:", raw.count())


# 2. Drop the Unwanted Columns

DROP_COLS = [
    "Unnamed: 0", "_inserted_at", "prop_$config_defaults",
    "prop_$is_identified", "prop_$web_vitals_enabled_server_side",
    "prop_$dead_clicks_enabled_server_side", "prop_$timezone_offset",
    "prop_$lib", "prop_$timezone", "prop_$insert_id", "prop_$time",
    "prop_$console_log_recording_enabled_server_side",
    "prop_$session_recording_start_reason", "prop_$raw_user_agent",
    "prop_$lib_version", "prop_$sent_at",
]

existing_drop = [c for c in DROP_COLS if c in raw.columns]
cleaned = raw.drop(*existing_drop)

print(f" Dropped {len(existing_drop)} columns")


# 3. Renaming of Columns

RENAME_MAP = {
    "uuid": "event_uuid",
    "event": "event_name",
    "timestamp": "event_timestamp",
    "prop_$user_id": "posthog_user_id",
    "prop_userId": "app_user_id",
    "prop_user_id": "prop_user_id",
    "prop_$session_id": "session_id",
    "prop_session_id": "prop_session_id",
    "prop_$pathname": "pathname",
    "prop_$current_url": "current_url",
    "prop_$host": "host",
    "prop_surface": "surface",
    "prop_$device_type": "device_type",
    "prop_$browser": "browser",
    "prop_$browser_version": "browser_version",
    "prop_$os": "os",
    "prop_$os_version": "os_version",
    "prop_$browser_language": "browser_language",
    "prop_$geoip_country_name": "country_name",
    "prop_$geoip_country_code": "country_code",
    "prop_$viewport_width": "viewport_width",
    "prop_$viewport_height": "viewport_height",
    "prop_$screen_width": "screen_width",
    "prop_$screen_height": "screen_height",
    "prop_$referrer": "referrer",
    "prop_$referring_domain": "referring_domain",
    "prop_tool_name": "tool_name",
    "prop_credit_amount": "credit_amount",
    "prop_credits_used": "credits_used",
}

for old, new in RENAME_MAP.items():
    if old in cleaned.columns:
        cleaned = cleaned.withColumnRenamed(old, new)


# 4. Dimensions


# dim_user
dim_user = (
    cleaned
    .select(
        "distinct_id",
        "person_id",
        F.coalesce("posthog_user_id", "app_user_id", "prop_user_id").alias("user_id")
    )
    .dropDuplicates(["distinct_id"])
    .withColumn("user_key", F.md5(F.col("distinct_id")).substr(1, 16))
)

# dim_device
dim_device = (
    cleaned
    .select("device_type", "browser", "browser_version", "os", "os_version", "browser_language")
    .dropDuplicates()
    .withColumn(
        "device_key",
        F.md5(
            F.concat_ws(
                "|",
                F.coalesce("device_type", F.lit("")),
                F.coalesce("browser", F.lit("")),
                F.coalesce("browser_version", F.lit("")),
                F.coalesce("os", F.lit("")),
                F.coalesce("os_version", F.lit("")),
                F.coalesce("browser_language", F.lit(""))
            )
        ).substr(1, 16)
    )
)

# dim_geo
dim_geo = (
    cleaned
    .select("country_name", "country_code")
    .dropDuplicates()
    .withColumn("geo_key", F.md5(F.coalesce("country_code", F.lit("UNKNOWN"))).substr(1, 16))
)

# dim_session
dim_session = (
    cleaned
    .withColumn("session_id", F.coalesce("session_id", "prop_session_id"))
    .select("session_id")
    .dropDuplicates()
    .withColumn("session_key", F.md5(F.coalesce(F.col("session_id"), F.lit("null"))).substr(1, 16))
)

# dim_page
dim_page = (
    cleaned
    .select("pathname", "current_url", "host", "surface")
    .dropDuplicates()
    .withColumn(
        "page_key",
        F.md5(
            F.concat_ws(
                "|",
                F.coalesce("host", F.lit("")),
                F.coalesce("pathname", F.lit(""))
            )
        ).substr(1, 16)
    )
)






# 5. Fact Table

fact_events = (
    cleaned
    .withColumn("user_key", F.md5(F.col("distinct_id")).substr(1, 16))
    .withColumn(
        "device_key",
        F.md5(
            F.concat_ws(
                "|",
                F.coalesce("device_type", F.lit("")),
                F.coalesce("browser", F.lit("")),
                F.coalesce("browser_version", F.lit("")),
                F.coalesce("os", F.lit("")),
                F.coalesce("os_version", F.lit("")),
                F.coalesce("browser_language", F.lit(""))
            )
        ).substr(1, 16)
    )
    .withColumn("geo_key", F.md5(F.coalesce("country_code", F.lit("UNKNOWN"))).substr(1, 16))
    .withColumn("session_key", F.md5(F.coalesce(F.coalesce("session_id", "prop_session_id"), F.lit("null"))).substr(1, 16))
    .withColumn(
        "page_key",
        F.md5(
            F.concat_ws(
                "|",
                F.coalesce("host", F.lit("")),
                F.coalesce("pathname", F.lit(""))
            )
        ).substr(1, 16)
    )
    .select(
        "event_uuid",
        "event_name",
        "event_timestamp",
        "user_key",
        "device_key",  
        "geo_key",
        "session_key",
        "page_key"
    )
)





# 6. Write it in Silver Layer

tables = {
    "dim_user": dim_user,
    "dim_device": dim_device,
    "dim_geo": dim_geo,
    "dim_session": dim_session,
    "dim_page": dim_page,
    "fact_events": fact_events,
}

for name, df in tables.items():
    path = f"s3a://{bucket}/silver/{name}/"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    print(f" Written {name}")

print("Silver layer processing completed")
spark.stop()

 
