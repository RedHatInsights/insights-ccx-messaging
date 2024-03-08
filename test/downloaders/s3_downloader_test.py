# Copyright 2024 Red Hat, Inc
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
"""Module containing unit tests for the `idp` class."""
import os
from moto import mock_aws
import boto3
from ccx_messaging.downloaders.s3_downloader import S3Downloader
@mock_aws
def test_download_existing_file():
    """Test downloading of existing file in s3."""
    file = open("testfile","a")
    file.write("test")
    file.close()
    conn = boto3.resource('s3',aws_access_key_id="test",aws_secret_access_key="test")
    bucket = conn.create_bucket(Bucket="testBucket")
    bucket.upload_file("testfile","testfile")
    downloader = S3Downloader(access_key="test",secret_key="test",bucket="testBucket",endpoint_url="https://s3.amazonaws.com")  # noqa: E501
    with downloader.get("testfile") as path:
      assert os.path.exists(path)
    os.remove("testfile")

@mock_aws
def test_download_non_existing_file():
    """Test downloading of non existing file in s3."""
    conn = boto3.resource('s3',aws_access_key_id="test",aws_secret_access_key="test")
    conn.create_bucket(Bucket="testBucket")
    downloader = S3Downloader(access_key="test",secret_key="test",bucket="testBucket",endpoint_url="https://s3.amazonaws.com")
    try:
        downloader.get("non_existend_file")
    except Exception as ex:
        assert isinstance(ex,FileNotFoundError)
