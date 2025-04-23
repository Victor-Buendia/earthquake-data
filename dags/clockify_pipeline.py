import datetime
import sys
import os
import io
import boto3
import botocore
import logging

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task

from pprint import pprint
from minio import Minio

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.ClockifyAPI import ClockifyInteractor

logger = logging.getLogger(__name__)

BUCKET_NAME = "raw"

with DAG(
    dag_id="clockify_pipeline",
    start_date=datetime.datetime(2025, 4, 1),
    schedule="@daily",
    catchup=False,
):

    @task(task_id="raw_clockify__time_entries")
    def clockify_ingestion(**kwargs):
        api = ClockifyInteractor(
            workspaceId=os.environ["CLOCKIFY_WORKSPACE_ID"], userId=os.environ["CLOCKIFY_USER_ID"]
        )
        res = api.get_time_entries().text

        client = boto3.client(
            "s3",
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
            verify=False,
            use_ssl=False,
            endpoint_url="http://minio:9000",
        )

        try:
            client.create_bucket(Bucket=BUCKET_NAME)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                logger.info("Bucket already exists. Skipping creation.")
            else:
                raise e

        result = client.put_object(
            Body=res.encode("utf-8"),
            Bucket=BUCKET_NAME,
            Key="data.json",
        )

        logger.info(result)
        return

    clockify = clockify_ingestion()
