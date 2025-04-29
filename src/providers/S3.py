import os
import boto3
import botocore
import logging


class S3BucketManager:
    def __init__(self, access_key, secret_key, endpoint_url):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            verify=False,
            use_ssl=False,
            endpoint_url=endpoint_url,
        )
        self.logger = logging.getLogger(__name__)

    def create_bucket(self, bucket_name):
        try:
            self.client.create_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                self.logger.info("Bucket already exists. Skipping creation.")
            else:
                raise e


if __name__ == "__main__":
    # Usage
    WAREHOUSE_BUCKET_NAME = "lakehouse"
    s3_manager = S3BucketManager(
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        endpoint_url="http://minio:9000",
    )
    s3_manager.create_bucket(WAREHOUSE_BUCKET_NAME)
