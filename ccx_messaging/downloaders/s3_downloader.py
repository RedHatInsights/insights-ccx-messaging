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

from insights_messaging.downloaders.s3 import S3Downloader as S3Downloader

class IDPDownloader(S3Downloader):

    """Downloader for S3 bucket."""

    def __init__(self, **kwargs):
        """Set up the S3 downloader."""
        if not kwargs['access_key']:
            raise ConfigurationError("Access Key environment variable not set.")
        if not kwargs['secret_key']:
            raise ConfigurationError("Secret Key environment variable not set.")
        if not kwargs['endpoint_url']:
            raise ConfigurationError("Endpoint environment variable not set.")
        if not kwargs['bucket']:
            raise ConfigurationError("Bucket environment variable not set.")
        self.access_key = kwargs['access_key']
        self.secret_key = kwargs['secret_key']
        self.endpoint_url = kwargs['endpoint_url']
        self.bucket = kwargs['bucket']
        super().__init__(key=self.access_key,secret=self.secret_key,client_kwargs={'endpoint_url':self.endpoint_url})

    def get(self, path):
        """Download the archive from given path."""
        return super().get(f"{self.bucket}/{path}")


class ConfigurationError(Exception):

    """Configuration error."""

    pass
