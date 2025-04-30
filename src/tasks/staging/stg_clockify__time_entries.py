import os
import sys


def stg_clockify__time_entries(RAW_BUCKET_NAME, WAREHOUSE_BUCKET_NAME, **kwargs):
    import logging
    from pyspark.sql import SparkSession
    from src.providers.S3 import S3BucketManager

    logger = logging.getLogger(__name__)

    S3BucketManager(
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
    ).create_bucket(WAREHOUSE_BUCKET_NAME)

    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("stg_clockify__time_entries")
        .getOrCreate()
    )

    data = spark.read.parquet(
        f"s3a://{RAW_BUCKET_NAME}/clockify/time-entries/parquet/*.parquet"
    )
    data.show()

    data.writeTo("clockify_catalog.bronze.time_entries").using(
        "iceberg"
    ).createOrReplace()


if __name__ == "__main__":
    RAW_BUCKET_NAME = sys.argv[1]
    WAREHOUSE_BUCKET_NAME = sys.argv[2]
    stg_clockify__time_entries(RAW_BUCKET_NAME, WAREHOUSE_BUCKET_NAME)
