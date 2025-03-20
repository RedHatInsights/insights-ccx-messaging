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

"""S3 Downloader for internal data pipeline."""

from contextlib import contextmanager

from insights_messaging.downloaders.s3 import S3Downloader as ICMS3Downloader

from ccx_messaging.error import CCXMessagingError


class S3Downloader(ICMS3Downloader):
    """Downloader for S3 bucket."""

    def __init__(self, **kwargs):
        """Set up the S3 downloader."""
        self.access_key = kwargs.get("access_key")
        self.secret_key = kwargs.get("secret_key")
        # 'endpoint_url' key is legacy and retained for backward compatibility.
        self.endpoint = kwargs.get("endpoint", kwargs.get("endpoint_url"))
        self.bucket = kwargs.get("bucket")
        if not self.access_key:
            raise CCXMessagingError("Access Key environment variable not set.")
        if not self.secret_key:
            raise CCXMessagingError("Secret Key environment variable not set.")
        if not self.endpoint:
            raise CCXMessagingError("Endpoint environment variable not set.")
        if not self.bucket:
            raise CCXMessagingError("Bucket environment variable not set.")
        super().__init__(
            key=self.access_key,
            secret=self.secret_key,
            client_kwargs={"endpoint_url": self.endpoint},
        )

    @contextmanager
    def get(self, path):
        """Download the archive from given path."""
        with super().get(f"{self.bucket}/{path}") as f:
            yield f
