import os

# from airflow.decorators import task


# @task.pyspark(task_id="stg_clockify__time_entries")
def stg_clockify__time_entries(STG_BUCKET_NAME, **kwargs):
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
        client.create_bucket(Bucket=STG_BUCKET_NAME)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logger.info("Bucket already exists. Skipping creation.")
        else:
            raise e

    spark = (
        SparkSession.builder.master("spark://spark:7077")
        .appName("stg_clockify__time_entries")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
        )
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.sql.catalog.clockify_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.clockify_catalog.type", "hadoop")
        .config(
            "spark.sql.catalog.clockify_catalog.warehouse", f"s3a://{STG_BUCKET_NAME}/iceberg/"
        )
        .getOrCreate()
    )

    data = spark.read.parquet("s3a://raw/clockify/time-entries/parquet/*.parquet")
    data.show()

    data.writeTo("clockify_catalog.bronze.time_entries").using("iceberg").createOrReplace()


if __name__ == "__main__":
    stg_clockify__time_entries("staging")
