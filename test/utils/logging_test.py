# Copyright 2023 Red Hat Inc.
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

"""Test for the ccx_messaging.utils.logging module."""

import logging
from unittest.mock import patch

import pytest
from watchtower import CloudWatchLogHandler

from ccx_messaging.utils.logging import (
    anonymize_message,
    anonymize_url,
    get_mac_address,
    setup_watchtower,
)

INVALID_INITIALIZATIONS = [
    {},
    {
        "LOGGING_TO_CW_ENABLED": "True",
    },
    {
        "LOGGING_TO_CW_ENABLED": "True",
        "CW_AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION_NAME": "aws-region1",
        "CW_LOG_GROUP": "log-group",
        "CW_STREAM_NAME": "stream",
    },
    {
        "LOGGING_TO_CW_ENABLED": "True",
        "CW_AWS_ACCESS_KEY_ID": "access_key",
        "AWS_REGION_NAME": "aws-region1",
        "CW_LOG_GROUP": "log-group",
        "CW_STREAM_NAME": "stream",
    },
    {
        "LOGGING_TO_CW_ENABLED": "True",
        "CW_AWS_ACCESS_KEY_ID": "access_key",
        "CW_AWS_SECRET_ACCESS_KEY": "secret",
        "CW_LOG_GROUP": "log-group",
        "CW_STREAM_NAME": "stream",
    },
    {
        "LOGGING_TO_CW_ENABLED": "True",
        "CW_AWS_ACCESS_KEY_ID": "access_key",
        "CW_AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION_NAME": "aws-region1",
        "CW_STREAM_NAME": "stream",
    },
    {
        "LOGGING_TO_CW_ENABLED": "True",
        "CW_AWS_ACCESS_KEY_ID": "access_key",
        "CW_AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION_NAME": "aws-region1",
        "CW_LOG_GROUP": "log-group",
    },
]


@pytest.mark.parametrize("invalid_environ", INVALID_INITIALIZATIONS)
def test_setup_watchtower_bad_configuration(invalid_environ):
    """Check that if no special env var is defined, cloudwatch logging is not configured."""
    root_logger = logging.getLogger()
    number_of_handlers = len(root_logger.handlers)

    with patch("os.environ", new=invalid_environ):
        setup_watchtower(None)
        assert number_of_handlers == len(root_logger.handlers)


# Set of valid environments to setup watchtower, no/info/wrong level (default to info)
VALID_INITIALIZATIONS = [
    {
        "LOGGING_TO_CW_ENABLED": "True",
        "CW_AWS_ACCESS_KEY_ID": "access_key",
        "CW_AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION_NAME": "aws-region1",
        "CW_LOG_GROUP": "log-group",
        "CW_STREAM_NAME": "stream",
    },
    {
        "LOGGING_TO_CW_ENABLED": "True",
        "CW_AWS_ACCESS_KEY_ID": "access_key",
        "CW_AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION_NAME": "aws-region1",
        "CW_LOG_GROUP": "log-group",
        "CW_STREAM_NAME": "stream",
        "CW_LOG_LEVEL": "INFO",
    },
    {
        "LOGGING_TO_CW_ENABLED": "True",
        "CW_AWS_ACCESS_KEY_ID": "access_key",
        "CW_AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION_NAME": "aws-region1",
        "CW_LOG_GROUP": "log-group",
        "CW_STREAM_NAME": "stream",
        "CW_LOG_LEVEL": "info",
    },
    {
        "LOGGING_TO_CW_ENABLED": "True",
        "CW_AWS_ACCESS_KEY_ID": "access_key",
        "CW_AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION_NAME": "aws-region1",
        "CW_LOG_GROUP": "log-group",
        "CW_STREAM_NAME": "stream",
        "CW_LOG_LEVEL": "NOT_A_LOG_LEVEL_AT_ALL",
    },
]


@pytest.mark.parametrize("environment", VALID_INITIALIZATIONS)
def test_setup_watchtower_info_level(environment):
    """Check that when a valid environment is defined, handler is added."""
    root_logger = logging.getLogger()
    number_of_handlers = len(root_logger.handlers)

    with patch("os.environ", new=environment):
        setup_watchtower(None)
        assert number_of_handlers + 1 == len(root_logger.handlers)
        for handler in root_logger.handlers:
            if isinstance(handler, CloudWatchLogHandler):
                cloudwatch_handler = handler
                break

        assert cloudwatch_handler.level == logging.INFO
        root_logger.removeHandler(cloudwatch_handler)


def test_get_mac_address_virtual():
    """Check that, if no real MAC is returned by the system, None is returned."""
    with patch("ccx_messaging.utils.logging.uuid.getnode") as getnode_mock:
        getnode_mock.return_value = 68039291015483  # magic number that does the trick
        assert get_mac_address() is None


def test_get_mac_address_real():
    """Check that, if no real MAC is returned by the system, None is returned."""
    with patch("ccx_messaging.utils.logging.uuid.getnode") as getnode_mock:
        getnode_mock.return_value = 66939779387710  # magic number that does the trick
        assert get_mac_address() == "3c:e1:a1:c5:91:3e"


# Each tuple is (url, expected_anonymized_url)
ANONYMIZE_URL_CASES = [
    (
        "https://insights-dev-upload-perm.s3.amazonaws.com/e927438c126040dab7891608447da0b5"
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256",
        "https://insights-dev-upload-perm.s3.amazonaws.com/****",
    ),
    (
        "https://user:pass@example.com:8443/some/path?query=1#frag",
        "https://****@example.com:8443/****",
    ),
    ("https://example.com", "https://example.com"),
    ("https://example.com/", "https://example.com/****"),
    ("http://example.com/path/to/archive.tar.gz", "http://example.com/****"),
    ("https://example.com?query=1", "https://example.com/****"),
    ("https://example.com#fragment", "https://example.com/****"),
    ("https://user@example.com", "https://****@example.com"),
    ("ftp://example.com/file.txt", "ftp://example.com/****"),
    ("https://example.com:8080", "https://example.com:8080"),
    ("", "****"),
    ("not a url", "****"),
    ("example.com/no-scheme", "****"),
    (None, "****"),
    (123, "****"),
    (b"http://example.com", "****"),
    (["not", "a", "url"], "****"),
    ("http://example.com:abc/path", "****"),
    ("http://example.com:99999999999999/path", "****"),
]


@pytest.mark.parametrize("url,expected", ANONYMIZE_URL_CASES)
def test_anonymize_url(url, expected):
    """Check that `anonymize_url` keeps the protocol/domain and hides auth/endpoint."""
    assert anonymize_url(url) == expected


# Each tuple is (input_message, expected_anonymized_message)
ANONYMIZE_MESSAGE_CASES = [
    (
        {
            "url": "https://user:pass@example.com/secret/path",
            "identity": {"identity": {"internal": {"org_id": "123"}}},
            "b64_identity": "eyJpZGVudGl0eSI6IHt9fQ==",
            "timestamp": "2020-01-23T16:15:59.478901889Z",
        },
        {
            "url": "https://****@example.com/****",
            "identity": "anonymized_value",
            "b64_identity": "anonymized_b64_identity",
            "timestamp": "2020-01-23T16:15:59.478901889Z",
        },
    ),
    (
        {"url": "https://example.com"},
        {"url": "https://example.com"},
    ),
    (
        {
            "identity": {"identity": {"internal": {"org_id": "123"}}},
            "b64_identity": "eyJpZGVudGl0eSI6IHt9fQ==",
        },
        {
            "identity": "anonymized_value",
            "b64_identity": "anonymized_b64_identity",
        },
    ),
    ({}, {}),
    (
        {"other": "kept as is"},
        {"other": "kept as is"},
    ),
]


@pytest.mark.parametrize("message,expected", ANONYMIZE_MESSAGE_CASES)
def test_anonymize_message(message, expected):
    """Check that `anonymize_message` anonymizes sensitive fields and keeps the rest."""
    assert anonymize_message(message) == expected


def test_anonymize_message_does_not_mutate_input():
    """Check that `anonymize_message` doesn't modify the original message dict."""
    message = {
        "url": "https://user:pass@example.com/secret",
        "identity": {"identity": {"internal": {"org_id": "123"}}},
        "b64_identity": "eyJpZGVudGl0eSI6IHt9fQ==",
    }
    original = message.copy()

    anonymize_message(message)

    assert message == original
