import logging
from contextlib import contextmanager

import boto3
from botocore.exceptions import ClientError


LOG = logging.getLogger(__name__)


class S3Uploader:
    def __init__(self, **kwargs):
        access_key = kwargs.pop("access_key")
        secret_key = kwargs.get("secret_key", None)
        endpoint = kwargs.get("endpoint", None)

        if not access_key:
            raise Exception("Access Key environment variable not set.")

        if not secret_key:
            raise Exception("Secret Key environment variable not set.")

        if not endpoint:
            raise Exception("Endpoint environment variable not set.")

        session = boto3.session.Session()

        self.client = session.client(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint,
        )

    def upload_file(self, path, bucket, file_name):
        LOG.info(f"Uploading '{file_name}' as '{path}' to '{bucket}'")

        with open(path, "rb") as file_data:
            self.client.put_object(Bucket=bucket, Key=file_name, Body=file_data)
            LOG.info(f"Uploaded '{file_name}' as '{path}' to '{bucket}'")
