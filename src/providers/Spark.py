import os
from pyspark.sql import SparkSession


class Spark:
    def __init__(self, app_name="spark"):
        self._spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName(app_name)
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
            )
            .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
            .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT_URL"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.sql.catalog.clockify_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.catalog.clockify_catalog.type", "hadoop")
            .config(
                "spark.sql.catalog.clockify_catalog.warehouse",
                f"s3a://{os.environ['WAREHOUSE_BUCKET_NAME']}/iceberg/",
            )
            .getOrCreate()
        )

    @property
    def session(self):
        return self._spark
