import logging
from contextlib import contextmanager
from insights_messaging.publishers import Publisher
import boto3
from botocore.exceptions import ClientError

LOG = logging.getLogger(__name__)


class IDPUploader(Publisher):

    def __init__(self, **kwargs):  # noqa: D107
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

    @contextmanager
    def upload_file(self, path, bucket, file_name):
        LOG.info(f"Uploading '{file_name}' as '{path}' to '{bucket}'")

        with open(path, "rb") as file_data:
            self.publish(bucket, file_data, file_name)
            LOG.info(f"Uploaded '{file_name}' as '{path}' to '{bucket}'")

    def publish(self, bucket, file_data, file_name):
        LOG.info(f"Uploading data to '{bucket}/{file_name}'")

        try:
            result = self.client.put_object(Bucket=bucket, Key=file_name, Body=file_data)
            LOG.debug(f"Upload result from '{bucket}/{file_name}': {result}")
            LOG.info(f"Uploaded data to '{bucket}/{file_name}'")
        except ClientError as err:
            LOG.error(
                "ClientError while uploading data"+str(err),
                extra={"bucket": bucket, "file_name": file_name, "err": err},
            )
