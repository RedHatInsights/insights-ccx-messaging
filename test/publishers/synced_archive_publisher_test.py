# Copyright 2024 Red Hat, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may naot use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the SyncedArchivePublisher class."""

import json
import gzip
from unittest.mock import MagicMock

import pytest
from ccx_messaging.publishers.synced_archive_publisher import SyncedArchivePublisher

BEST_COMPRESSION = 9

INPUT_MSG = [
    pytest.param(
        {
            "path": "target/path",
            "original_path": "original/path",
            "metadata": {"cluster_id": "12345", "external_organization": "54321"},
        }
    )
]


def timeStampMasking(message):
    """Mask four bytes in Gzip stream that contain timestamp."""
    message = list(message)
    message[4] = 0
    message[5] = 0
    message[6] = 0
    message[7] = 0
    message = bytes(message)
    return message


def test_init():
    """Check that init creates a valid object."""
    kakfa_config = {
        "bootstrap.servers": "kafka:9092",
    }
    SyncedArchivePublisher(outgoing_topic="topic name", **kakfa_config)


@pytest.mark.parametrize("input", INPUT_MSG)
def test_compressing_disabled(input):
    """Check if message is not gzipped if compression is disabled."""
    input = bytes(json.dumps(input) + "\n", "utf-8")
    expected_output = input
    kakfa_config = {"bootstrap.servers": "kafka:9092"}
    pub = SyncedArchivePublisher(outgoing_topic="topic-name", **kakfa_config)
    pub.producer = MagicMock()
    pub.produce(input)
    pub.producer.produce.assert_called_with("topic-name", expected_output)


@pytest.mark.parametrize("input", INPUT_MSG)
def test_compressing_enabled(input):
    """Check if message is gzipped if compression is enabled."""
    input = bytes(json.dumps(input) + "\n", "utf-8")
    expected_output = timeStampMasking(gzip.compress(input, compresslevel=BEST_COMPRESSION))
    kakfa_config = {"bootstrap.servers": "kafka:9092", "compression": "gzip"}
    pub = SyncedArchivePublisher(outgoing_topic="topic-name", **kakfa_config)
    pub.producer = MagicMock()
    pub.produce(input)
    outgoing_topic = pub.producer.produce.call_args[0][0]
    outgoing_message = timeStampMasking(pub.producer.produce.call_args[0][1])
    assert outgoing_message == expected_output and outgoing_topic == "topic-name"
