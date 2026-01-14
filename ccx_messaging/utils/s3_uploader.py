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

    def __init__(self, access_key, secret_key, endpoint, acl_enabled=True):
        """Inicialize uploader."""
        if not access_key:
            raise TypeError("access_key cannot be nulleable")

        if not secret_key:
            raise TypeError("secret_key cannot be nulleable")

        session = boto3.session.Session()

        self.client = session.client(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint,
        )

        self.acl_enabled = acl_enabled

    def upload_file(self, path, bucket, file_name):
        """Upload file to target path in bucket."""
        LOG.debug(f"Uploading '{file_name}' as '{path}' to '{bucket}'")

        with open(path, "rb") as file_data:
            if self.acl_enabled:
                self.client.put_object(
                    Bucket=bucket, Key=file_name, Body=file_data, ACL="bucket-owner-read"
                )
            else:
                self.client.put_object(Bucket=bucket, Key=file_name, Body=file_data)
            LOG.debug(f"Uploaded '{file_name}' as '{path}' to '{bucket}'")
