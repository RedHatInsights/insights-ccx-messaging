# Copyright 2020, 2021, 2022 Red Hat, Inc
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

"""Module containing unit tests for the `HTTPDownloader` class."""

from unittest.mock import MagicMock, patch

import pytest

from ccx_messaging.downloaders.http_downloader import HTTPDownloader, parse_human_input
from ccx_messaging.error import CCXMessagingError


_INVALID_FILE_SIZES = [42, 2.71, True, [], {}, "1234J"]
_VALID_FILE_SIZES = ["1K", "2M", "3G", "4T", "5Ki", "6Mi", "7Gi", "8Ti"]


@pytest.mark.parametrize("file_size", _INVALID_FILE_SIZES)
def test_parse_human_input_invalid_file_sizes(file_size):
    """Test that passing invalid data to parse_human_input raise an exception."""
    with pytest.raises((TypeError, ValueError)):
        parse_human_input(file_size)


@pytest.mark.parametrize("file_size", _VALID_FILE_SIZES)
def test_parse_human_input_valid_file_sizes(file_size):
    """Test that passing valid data to parse_human_input doesn't raise an exception."""
    parse_human_input(file_size)


_REGEX_BAD_URL_FORMAT = r"^Invalid URL format"
_INVALID_TYPE_URLS = [42, 2.71, True, [], {}]


@pytest.mark.parametrize("url", _INVALID_TYPE_URLS)
def test_get_invalid_type(url):
    """Test that passing invalid data type to `get` raises an exception."""
    with pytest.raises(TypeError):
        sut = HTTPDownloader()
        with sut.get(url):
            pass


_INVALID_URLS = [None, "", "ftp://server", "bucket/file"]


@pytest.mark.parametrize("url", _INVALID_URLS)
def test_get_invalid_url(url):
    """Test that passing invalid URL to `get` raises an exception."""
    with pytest.raises(CCXMessagingError, match=_REGEX_BAD_URL_FORMAT):
        sut = HTTPDownloader()
        with sut.get(url):
            pass


_VALID_URLS = [
    "https://zzzzzzzzzzzzzzzzzzzzzzzz.s3.amazonaws.com/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential="
    "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ&X-Amz-Date=19700101T000000Z"
    "&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature="
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "https://insights-dev-upload-perm.s3.amazonaws.com/upload-service-1-48nb7/gPRz2EdWpr-000144"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=FillInCorrectDataHere%2F2C2D0311%2F"
    "us-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200311T080728Z&X-Amz-Expires=86400&"
    "X-Amz-SignedHeaders=host&"
    "X-Amz-Signature=0000cafe0000babe0000cafe0000babe0000cafe0000babe0000cafe0000babe",
    "http://minio:9000/insights-upload-perma/server.in.my.company.com/"
    "Z0ThU1Jyxc-000004?X-Amz-Algorithm=AWS4-HMAC-SHA256&"
    "X-Amz-Credential=ThisIsNotCorrectCred%2F20200520%2Fus-east-1%2Fs3%2Faws4_request&"
    "X-Amz-Date=20200520T140918Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&"
    "X-Amz-Signature=0000cafe0000babe0000cafe0000babe0000cafe0000babe0000cafe0000babe",
    "https://s3.us-east-1.amazonaws.com/insights-ingress-prod/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential="
    "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ&X-Amz-Date=20201201T210535Z&"
    "X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature="
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
]


@patch("requests.get")
@pytest.mark.parametrize("url", _VALID_URLS)
def test_get_valid_url(get_mock, url):
    """Test that passing a valid URL the `get` method tries to download it."""
    response_mock = MagicMock()
    get_mock.return_value = response_mock
    response_mock.content = b"file content"
    sut = HTTPDownloader()

    with sut.get(url) as filename:
        with open(filename, "rb") as file_desc:
            file_content = file_desc.read()
            assert file_content == response_mock.content
