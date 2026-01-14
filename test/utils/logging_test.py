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

from ccx_messaging.utils.logging import get_mac_address, setup_watchtower


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
