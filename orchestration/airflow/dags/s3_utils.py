import boto3
import os

BUCKET = os.getenv("AWS_BUCKET", "news-pipeline-data")
s3 = boto3.client("s3")

def upload_to_s3(local_file, s3_key):
    """Uploads a single file to S3."""
    try:
        s3.upload_file(local_file, BUCKET, s3_key)
        print(f" Uploaded {local_file} → s3://{BUCKET}/{s3_key}")
    except Exception as e:
        print(f" Failed to upload {local_file} to S3: {e}")
