import os
import uuid
import boto3
from prefect.blocks.system import Secret


def _client():
    endpoint = Secret.load("s3-endpoint").get()
    access_key = Secret.load("s3-access-key").get()
    secret_key = Secret.load("s3-secret-key").get()
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def download_import_file(s3_key: str, tmp_directory: str = ".tmp") -> str:
    """Downloads a file from the private imports bucket to a local tmp path, returns that path."""
    bucket = Secret.load("s3-imports-bucket").get()
    os.makedirs(tmp_directory, exist_ok=True)
    local_path = os.path.join(tmp_directory, f"import_{uuid.uuid4().hex}.zip")

    client = _client()
    client.download_file(bucket, s3_key, local_path)
    return local_path
