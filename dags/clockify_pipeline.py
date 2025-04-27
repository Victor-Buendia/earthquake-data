import sys
import os
import io
import boto3
import botocore
import logging
import pandas as pd
import shutil
import json

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.models.param import Param

from pprint import pprint
from minio import Minio
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.ClockifyAPI import ClockifyInteractor

logger = logging.getLogger(__name__)

BUCKET_NAME = "raw"

with DAG(
    dag_id="clockify_pipeline",
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    catchup=True
) as dag:

    @task(task_id="raw_clockify__time_entries")
    def clockify_ingestion(ti=None, **kwargs):
        api = ClockifyInteractor(
            workspaceId=os.environ["CLOCKIFY_WORKSPACE_ID"],
            userId=os.environ["CLOCKIFY_USER_ID"],
        )
        
        task_start_date: date = datetime.date(kwargs["data_interval_start"])
        task_id = ti

        res = api.get_time_entries(start=datetime.strftime(task_start_date, "%Y-%m-%dT%H:%M:%SZ"), end=datetime.strftime(task_start_date + timedelta(days=1), "%Y-%m-%dT%H:%M:%SZ"))
        if res.status_code != 200:
            raise Exception(res.text)

        json_data = json.loads(res.text)
        df = pd.DataFrame(json_data)

        file_path = f"/tmp/{BUCKET_NAME}/{task_start_date.isoformat()}.parquet"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        data = df.to_parquet(file_path)

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

        with open(file_path, "rb") as f:
            result = client.put_object(
                Bucket=BUCKET_NAME,
                Key=f"clockify/time-entries/{datetime.strftime(task_start_date, '%Y-%m-%d')}.parquet",
                Body=f,
            )

        logger.info(result)

        return

    clockify = clockify_ingestion()
