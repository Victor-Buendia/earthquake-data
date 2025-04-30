import logging
import sys

from datetime import datetime

logger = logging.getLogger(__name__)


def raw_clockify__time_entries__parquet(BUCKET_NAME, **kwargs):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("raw_clockify__time_entries__parquet")
        .getOrCreate()
    )

    task_start_date: date = datetime.strptime(kwargs["data_interval_start"], "%Y-%m-%d")

    df = spark.read.json(
        f"s3a://{BUCKET_NAME}/clockify/time-entries/{datetime.strftime(task_start_date, '%Y-%m-%d')}.json"
    )
    if df.count() > 0:
        df.write.mode("overwrite").parquet(
            f"s3a://{BUCKET_NAME}/clockify/time-entries/parquet/{datetime.strftime(task_start_date, '%Y-%m-%d')}.parquet"
        )


if __name__ == "__main__":
    RAW_BUCKET_NAME = sys.argv[1]
    data_interval_start = sys.argv[2]

    raw_clockify__time_entries__parquet(
        RAW_BUCKET_NAME, data_interval_start=data_interval_start
    )
