# Copyright 2024 Red Hat Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Class for s3 uploader."""
import logging
import boto3


LOG = logging.getLogger(__name__)


class S3Uploader:
    """S3 uploader."""

    def __init__(self, **kwargs):
        """Inicialize uploader."""
        access_key = kwargs.get("access_key", None)
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
        """Upload file to target path in bucket."""
        LOG.info(f"Uploading '{file_name}' as '{path}' to '{bucket}'")

        with open(path, "rb") as file_data:
            self.client.put_object(Bucket=bucket, Key=file_name, Body=file_data)
            LOG.info(f"Uploaded '{file_name}' as '{path}' to '{bucket}'")
