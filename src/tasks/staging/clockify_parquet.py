import os
import boto3
import logging
import json
import pandas as pd

from airflow.decorators import task

from datetime import datetime

logger = logging.getLogger(__name__)

BUCKET_NAME = "raw"

@task(task_id="raw_clockify__time_entries__parquet")
def clockify_parquet(BUCKET_NAME, **kwargs):
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
