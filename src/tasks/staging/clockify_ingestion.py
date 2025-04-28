import sys
import os
import boto3
import botocore
import logging

from airflow.decorators import task

from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.ClockifyAPI import ClockifyInteractor

logger = logging.getLogger(__name__)

@task(task_id="raw_clockify__time_entries")
def clockify_ingestion(BUCKET_NAME, **kwargs):
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