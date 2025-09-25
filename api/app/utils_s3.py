# api/app/utils_s3.py
import boto3
from botocore.client import Config

def get_s3_client(endpoint, access_key, secret_key):
    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )

def ensure_bucket(s3, bucket):
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass
