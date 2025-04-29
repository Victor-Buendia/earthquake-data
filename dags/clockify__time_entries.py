import sys
import os
import logging

from airflow.models.dag import DAG
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.tasks.raw.raw_clockify__time_entries import raw_clockify__time_entries
from src.tasks.raw.raw_clockify__time_entries__parquet import (
    raw_clockify__time_entries__parquet,
)
from src.tasks.staging.stg_clockify__time_entries import stg_clockify__time_entries

logger = logging.getLogger(__name__)

RAW_BUCKET_NAME = "raw"
WAREHOUSE_BUCKET_NAME = "lakehouse"

with DAG(
    dag_id="clockify__time_entries__pipeline",
    start_date=datetime(2025, 4, 20),
    schedule="@daily",
    catchup=True,
) as dag:

    raw_clockify__time_entries = raw_clockify__time_entries(RAW_BUCKET_NAME)
    raw_clockify__time_entries__parquet = raw_clockify__time_entries__parquet(
        RAW_BUCKET_NAME
    )

    stg_clockify__time_entries = SparkSubmitOperator(
        task_id="stg_clockify__time_entries",
        application="/opt/airflow/src/tasks/staging/stg_clockify__time_entries.py",
        conn_id="spark_docker",
        application_args=[RAW_BUCKET_NAME, WAREHOUSE_BUCKET_NAME],
        conf={
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
            "spark.hadoop.fs.s3a.access.key": os.environ["MINIO_ACCESS_KEY"],
            "spark.hadoop.fs.s3a.secret.key": os.environ["MINIO_SECRET_KEY"],
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.catalog.clockify_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.clockify_catalog.type": "hadoop",
            "spark.sql.catalog.clockify_catalog.warehouse": f"s3a://{WAREHOUSE_BUCKET_NAME}/iceberg/",
        },
        verbose=True,
    )

    raw_clockify__time_entries >> raw_clockify__time_entries__parquet
    raw_clockify__time_entries__parquet >> stg_clockify__time_entries
