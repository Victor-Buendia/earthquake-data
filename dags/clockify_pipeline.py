import sys
import os
import boto3
import botocore
import logging
import json
import pandas as pd

from airflow.models.dag import DAG
from airflow.decorators import dag, task

from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.ClockifyAPI import ClockifyInteractor

logger = logging.getLogger(__name__)

BUCKET_NAME = "raw"

with DAG(
    dag_id="clockify_pipeline",
    start_date=datetime(2025, 4, 20),
    schedule="@daily",
    catchup=True,
) as dag:

    @task(task_id="raw_clockify__time_entries")
    def clockify_ingestion(**kwargs):
        api = ClockifyInteractor(
            workspaceId=os.environ["CLOCKIFY_WORKSPACE_ID"],
            userId=os.environ["CLOCKIFY_USER_ID"],
        )

        task_start_date: date = datetime.date(kwargs["data_interval_start"])

        res = api.get_time_entries(
            start=datetime.strftime(task_start_date, "%Y-%m-%dT%H:%M:%SZ"),
            end=datetime.strftime(
                task_start_date + timedelta(days=1), "%Y-%m-%dT%H:%M:%SZ"
            ),
        )
        if res.status_code != 200:
            raise Exception(res.text)
        elif len(res.text.encode("utf-8")) == 0:
            raise Exception("No data found")

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
            Bucket=BUCKET_NAME,
            Key=f"clockify/time-entries/{datetime.strftime(task_start_date, '%Y-%m-%d')}.json",
            Body=res.text.encode("utf-8"),
        )

        logger.info(result)

        return

    @task(task_id="raw_clockify__time_entries__parquet")
    def clockify_parquet(**kwargs):
        task_start_date: date = datetime.date(kwargs["data_interval_start"])

        client = boto3.client(
            "s3",
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
            verify=False,
            use_ssl=False,
            endpoint_url="http://minio:9000",
        )

        data = client.get_object(
            Bucket=BUCKET_NAME,
            Key=f"clockify/time-entries/{datetime.strftime(task_start_date, '%Y-%m-%d')}.json",
        )

        data = json.loads(data["Body"].read().decode("utf-8"))
        if len(data) == 0:
            logger.error("No data found")
            return

        folder_path = f"/tmp/{datetime.now().isoformat()}/{BUCKET_NAME}/clockify/time-entries/parquet"
        os.makedirs(folder_path, exist_ok=True)
        print(pd.DataFrame(data))
        pd.DataFrame(data).to_parquet(os.path.join(folder_path, f"{datetime.strftime(task_start_date, '%Y-%m-%d')}.parquet"))

        with open(os.path.join(folder_path, f"{datetime.strftime(task_start_date, '%Y-%m-%d')}.parquet"), "rb") as f:
            client.put_object(
                Bucket=BUCKET_NAME,
                Key=f"clockify/time-entries/parquet/{datetime.strftime(task_start_date, '%Y-%m-%d')}.parquet",
                Body=f.read()
            )


    raw = clockify_ingestion()
    parquet = clockify_parquet()

    raw >> parquet
