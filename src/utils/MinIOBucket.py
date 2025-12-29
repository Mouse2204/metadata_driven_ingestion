import boto3
import os
from botocore.exceptions import ClientError

def init_minio_buckets():
    minio_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    buckets = ["raw-data", "delta-lake", "processed-data"]

    for bucket_name in buckets:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Checked bucket: {bucket_name}")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"]:
                print(f"Bucket exists: {bucket_name}")
            else:
                print(f"Error creating bucket {bucket_name}: {e}")