# Copyright 2025 Red Hat, Inc
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

"""Module containing unit tests for the `KafkaMultiplexorConsumer` class."""

import datetime
import json
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from ccx_messaging.consumers.decoded_ingress_consumer import DecodedIngressConsumer
from ccx_messaging.error import CCXMessagingError

from . import KafkaMessage


_INVALID_TYPE_VALUES = [
    None,
    42,
    3.14,
    True,
    [],
    {},
]


@pytest.mark.parametrize("value", _INVALID_TYPE_VALUES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_invalid_type(mock_consumer, value):
    """Test that passing invalid data type to `deserialize` raises an exception."""
    sut = DecodedIngressConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(value)


@pytest.mark.parametrize("value", _INVALID_TYPE_VALUES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_invalid_type_as_kafka_message(mock_consumer, value):
    """Test that passing invalid data type to `deserialize` raises an exception."""
    sut = DecodedIngressConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(KafkaMessage(value))


_INVALID_MESSAGES = [
    "",
    "{}",
    '{"noturl":"https://s3.com/hash"}',
    '{"url":"value"',
    '"url":"value"}',
    '"url":"value"',
    '"{"url":"value"}"',
    # incorrect identity
    '{"url": "https://s3.com/hash", '
    '"identity": {"external": {"internal": {"org_id": "12345678"}}}, '
    '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
    '{"url": "https://s3.com/hash", '
    '"identity": {"internal": {"internal": {"orgs": "12345678"}}}, '
    '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
    # incorrect cluster_name
    '{"url": "https://s3.com/hash", '
    '"identity": {"identity": {"internal": {"org_id": "12345678"}}}, '
    '"timestamp": "2020-01-23T16:15:59.478901889Z", '
    '"cluster_name": 1}',
]


@pytest.mark.parametrize("msg", _INVALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_invalid_format_str(mock_consumer, msg):
    """Test that passing a malformed message to `deserialize` raises an exception."""
    sut = DecodedIngressConsumer(None, None, None, None)
    message = KafkaMessage(msg)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(message)


_VALID_MESSAGES = [
    '{"url": "https://s3.com/hash", '
    '"identity": {"identity": {"internal": {"org_id": "12345678"}}}, '
    '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
    '{"url": "https://s3.com/hash", '
    '"identity": {"identity": {"internal": {"org_id": "12345678"}}}, '
    '"timestamp": "2020-01-23T16:15:59.478901889Z", '
    '"cluster_name": "c9d116ce-93db-4c19-abe3-0de1d3554f99"}',
    # null cluster
    '{"url": "https://s3.com/hash", '
    '"identity": {"identity": {"internal": {"org_id": "12345678"}}}, '
    '"timestamp": "2020-01-23T16:15:59.478901889Z", '
    '"cluster_name": null}',
]


@pytest.mark.parametrize("msg", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_str(msg):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = DecodedIngressConsumer(None, None, None, incoming_topic=None)
    out = json.loads(msg)
    if "cluster_name" not in msg:
        out["cluster_name"] = None
    assert sut.deserialize(KafkaMessage(msg)) == out


@pytest.mark.parametrize("msg", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_bytes(msg):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = DecodedIngressConsumer(None, None, None, incoming_topic=None)
    out = json.loads(msg)
    if "cluster_name" not in msg:
        out["cluster_name"] = None
    assert sut.deserialize(KafkaMessage(msg.encode())) == out


@pytest.mark.parametrize("msg", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_bytearray(msg):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = DecodedIngressConsumer(None, None, None, incoming_topic=None)
    out = json.loads(msg)
    if "cluster_name" not in msg:
        out["cluster_name"] = None
    assert sut.deserialize(KafkaMessage(bytearray(msg.encode()))) == out


@pytest.mark.parametrize("msg", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_handles_valid(msg):
    """Test that `handles` method returns True for valid messages."""
    sut = DecodedIngressConsumer(None, None, None, incoming_topic=None)
    assert sut.handles(KafkaMessage(msg))


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
@patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.handles", lambda *a, **k: True)
@patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.fire", lambda *a, **k: None)
def test_last_received_message_time_is_updated():
    """Check that the variable last_received_message_time used by a Thread is correctly updated.

    [CCXDEV-14812] the variable is never updated
    """
    t1 = datetime.datetime(2025, 1, 31, 12, 0, 0, tzinfo=datetime.timezone.utc)
    t2 = datetime.datetime(2025, 1, 31, 12, 20, 0, tzinfo=datetime.timezone.utc)

    with freeze_time(t1) as frozen_time:
        sut = DecodedIngressConsumer(None, None, None, incoming_topic=None)
        input_msg = KafkaMessage("{}")

        frozen_time.move_to(t2)

        with patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process", lambda: None):
            sut.process_msg(input_msg)

        assert sut.last_received_message_time == t2.timestamp()


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_extract_cluster_id():
    """Check that the cluster_id is extracted from the identity field when not available.

    [CCXDEV-15056] Missing cluster id in some messages
    """
    sut = DecodedIngressConsumer(None, None, None, incoming_topic=None)
    cluster_id = "c9d116ce-93db-4c19-abe3-0de1d3554f99"
    msg = {
        "url": "https://s3.com/hash",
        "identity": {
            "identity": {
                "system": {
                    "cluster_id": cluster_id,
                },
                "internal": {
                    "org_id": "1234",
                },
            },
        },
        "timestamp": "2020-01-23T16:15:59.478901889Z",
    }

    assert sut.deserialize(KafkaMessage(json.dumps(msg)))["cluster_name"] == cluster_id
