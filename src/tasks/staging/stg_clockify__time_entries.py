import os
import sys

def stg_clockify__time_entries(RAW_BUCKET_NAME, WAREHOUSE_BUCKET_NAME, **kwargs):
    import boto3
    import botocore
    import logging
    from pyspark.sql import SparkSession

    logger = logging.getLogger(__name__)

    client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        verify=False,
        use_ssl=False,
        endpoint_url="http://minio:9000",
    )

    try:
        client.create_bucket(Bucket=WAREHOUSE_BUCKET_NAME)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logger.info("Bucket already exists. Skipping creation.")
        else:
            raise e

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
