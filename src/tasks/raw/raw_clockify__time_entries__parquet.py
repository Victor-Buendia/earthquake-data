import os
import logging
import json
import pandas as pd

from airflow.decorators import task

from datetime import datetime

from src.providers.S3 import S3BucketManager

logger = logging.getLogger(__name__)

BUCKET_NAME = "raw"


@task(task_id="raw_clockify__time_entries__parquet")
def raw_clockify__time_entries__parquet(BUCKET_NAME, **kwargs):
    task_start_date: date = datetime.date(kwargs["data_interval_start"])

    s3_manager = S3BucketManager(
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
    )

    data = s3_manager.client.get_object(
        Bucket=BUCKET_NAME,
        Key=f"clockify/time-entries/{datetime.strftime(task_start_date, '%Y-%m-%d')}.json",
    )

    data = json.loads(data["Body"].read().decode("utf-8"))
    if len(data) == 0:
        logger.error("No data found")
        return

    folder_path = (
        f"/tmp/{datetime.now().isoformat()}/{BUCKET_NAME}/clockify/time-entries/parquet"
    )
    os.makedirs(folder_path, exist_ok=True)
    print(pd.DataFrame(data))
    pd.DataFrame(data).to_parquet(
        os.path.join(
            folder_path, f"{datetime.strftime(task_start_date, '%Y-%m-%d')}.parquet"
        )
    )

    with open(
        os.path.join(
            folder_path, f"{datetime.strftime(task_start_date, '%Y-%m-%d')}.parquet"
        ),
        "rb",
    ) as f:
        s3_manager.client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"clockify/time-entries/parquet/{datetime.strftime(task_start_date, '%Y-%m-%d')}.parquet",
            Body=f.read(),
        )
