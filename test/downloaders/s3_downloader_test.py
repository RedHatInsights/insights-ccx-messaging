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
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pytest

from ccx_messaging.downloaders.s3_downloader import S3Downloader


@patch("s3fs.S3FileSystem.open")
def test_download_existing_file(mock_s3_open):
    """Test downloading of existing file in s3."""
    downloader = S3Downloader(
        access_key="test",
        secret_key="test",
        bucket="testBucket",
        endpoint_url="https://s3.amazonaws.com",
    )

    test_file_name = ""

    with NamedTemporaryFile(delete=False) as test_file:
        test_file.write(b"Hello world")
        test_file_name = test_file.name

    with open(test_file_name, "rb") as fd:
        mock_s3_open.return_value.__enter__.return_value = fd
        with downloader.get("testfile") as path:
            assert os.path.exists(path)

    os.unlink(test_file_name)


@patch("s3fs.S3FileSystem.open")
def test_download_non_existing_file(mock_s3_open):
    """Test downloading of non existing file in s3."""
    downloader = S3Downloader(
        access_key="test",
        secret_key="test",
        bucket="testBucket",
        endpoint_url="https://s3.amazonaws.com",
    )

    mock_s3_open.side_effect = FileNotFoundError()

    with pytest.raises(FileNotFoundError):
        with downloader.get("non_existend_file"):
            pytest.fail()
