from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os 


def upload_csv_to_s3(**context):
    import boto3
    import os

    s3 = boto3.resource(
        service_name='s3',
        region_name=os.getenv("AWS_DEFAULT_REGION", "ap-south-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    s3.Bucket('rawdataingestiontest1').upload_file(
        Filename='/opt/airflow/data/zerve_hackathon_for_reviewc8fa7c7.csv',
        Key="raw/Database.csv"
    )
    print("File uploaded to S3 bucket")


with DAG(
    dag_id="posthog_simple_pipeline8",
    start_date=datetime(2024, 1, 1),
    schedule="@daily", 
    catchup=False,
) as dag:

    upload_task = PythonOperator(
        task_id='upload_raw_to_s3',
        python_callable=upload_csv_to_s3,
        dag=dag,
    )

    silver = SparkSubmitOperator(
        task_id="silver_processing",
        application="/opt/airflow/scripts/silver_processing_simple.py",
        application_args=[
            "rawdataingestiontest1",
            "{{ ds_nodash }}"
        ],
        conn_id="spark_default",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        env_vars={
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'AWS_DEFAULT_REGION': os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        }
    )

    gold = SparkSubmitOperator(
        task_id="gold_processing",
        application="/opt/airflow/scripts/gold_processing_simple.py",
        application_args=["rawdataingestiontest1"],
        conn_id="spark_default",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
       
    )

    upload_task >> silver >> gold

