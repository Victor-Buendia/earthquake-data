import os
import logging

from airflow.decorators import task

from datetime import datetime, timedelta

from src.ClockifyAPI import ClockifyInteractor
from src.providers.S3 import S3BucketManager

logger = logging.getLogger(__name__)


@task(task_id="raw_clockify__time_entries")
def raw_clockify__time_entries(BUCKET_NAME, **kwargs):
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

    s3_manager = S3BucketManager(
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
    )
    s3_manager.create_bucket(BUCKET_NAME)

    result = s3_manager.client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"clockify/time-entries/{datetime.strftime(task_start_date, '%Y-%m-%d')}.json",
        Body=res.text.encode("utf-8"),
    )

    logger.info(result)

    return
