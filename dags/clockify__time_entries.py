import os
import logging

from datetime import datetime

from airflow.models.dag import DAG
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from src.tasks.raw.raw_clockify__time_entries import raw_clockify__time_entries
from src.tasks.staging.stg_clockify__time_entries import stg_clockify__time_entries
from src.tasks.raw.raw_clockify__time_entries__parquet import (
    raw_clockify__time_entries__parquet,
)

logger = logging.getLogger(__name__)

RAW_BUCKET_NAME = os.environ["RAW_BUCKET_NAME"]
WAREHOUSE_BUCKET_NAME = os.environ["WAREHOUSE_BUCKET_NAME"]

with DAG(
    dag_id="clockify__time_entries__pipeline",
    start_date=datetime(2025, 4, 22),
    end_date=datetime(2025, 4, 23),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
) as dag:

    jars = []
    for a, b, c in os.walk("/opt/bitnami/spark/jars"):
        for jar in c:
            jars.append(os.path.join(a, jar))
    jars = ",".join(jars)

    raw_clockify__time_entries = raw_clockify__time_entries(RAW_BUCKET_NAME)
    raw_clockify__time_entries__parquet = SparkSubmitOperator(
        task_id="raw_clockify__time_entries__parquet",
        application="/opt/airflow/src/tasks/raw/raw_clockify__time_entries__parquet.py",
        conn_id="spark_docker",
        application_args=[RAW_BUCKET_NAME, "{{ ds }}"],
        conf={
            "spark.jars": jars,
            "spark.hadoop.fs.s3a.access.key": os.environ["MINIO_ACCESS_KEY"],
            "spark.hadoop.fs.s3a.secret.key": os.environ["MINIO_SECRET_KEY"],
            "spark.hadoop.fs.s3a.endpoint": os.environ["MINIO_ENDPOINT_URL"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        verbose=True,
    )

    stg_clockify__time_entries = SparkSubmitOperator(
        task_id="stg_clockify__time_entries",
        application="/opt/airflow/src/tasks/staging/stg_clockify__time_entries.py",
        conn_id="spark_docker",
        application_args=[RAW_BUCKET_NAME, WAREHOUSE_BUCKET_NAME],
        conf={
            "spark.jars": jars,
            "spark.hadoop.fs.s3a.access.key": os.environ["MINIO_ACCESS_KEY"],
            "spark.hadoop.fs.s3a.secret.key": os.environ["MINIO_SECRET_KEY"],
            "spark.hadoop.fs.s3a.endpoint": os.environ["MINIO_ENDPOINT_URL"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.catalog.clockify_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.clockify_catalog.type": "hive",
            "spark.sql.catalog.clockify_catalog.uri": "thrift://metastore:9083",
            "spark.sql.catalog.clockify_catalog.warehouse": f"s3a://{os.environ['WAREHOUSE_BUCKET_NAME']}/warehouse",
        },
        verbose=True,
    )

    raw_clockify__time_entries >> raw_clockify__time_entries__parquet
    raw_clockify__time_entries__parquet >> stg_clockify__time_entries
