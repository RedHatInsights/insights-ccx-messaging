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

"""Module containing unit tests for the `KafkaConsumer` class."""

import datetime
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from ccx_messaging.consumers.idp_kafka_consumer import IDPConsumer
from ccx_messaging.error import CCXMessagingError

from . import KafkaMessage


# _REGEX_BAD_SCHEMA = r"^Unable to extract URL from input message: "
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
    sut = IDPConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(value)


@pytest.mark.parametrize("value", _INVALID_TYPE_VALUES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_invalid_type_as_kafka_message(mock_consumer, value):
    """Test that passing invalid data type to `deserialize` raises an exception."""
    sut = IDPConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(KafkaMessage(value))


_VALID_MESSAGES = [
    (
        '{"path": ""}',
        {
            "path": "",
        },
    ),
    (
        '{"path": "","cluster_id": "3d59cd30-e2c3-4ec8-b35d-7df3b1feea2f"}',
        {
            "path": "",
            "cluster_id": "3d59cd30-e2c3-4ec8-b35d-7df3b1feea2f",
        },
    ),
    (
        '{"path": "",'
        '"cluster_id": "3d59cd30-e2c3-4ec8-b35d-7df3b1feea2f",'
        '"sqs_message_id": "a_very_long_sqs_id"'
        "}",
        {
            "path": "",
            "cluster_id": "3d59cd30-e2c3-4ec8-b35d-7df3b1feea2f",
            "sqs_message_id": "a_very_long_sqs_id",
        },
    ),
    (
        '{"path": "path/inside/bucket/file.tgz",'
        '"cluster_id": "3d59cd30-e2c3-4ec8-b35d-7df3b1feea2f",'
        '"sqs_message_id": "a_very_long_sqs_id"'
        "}",
        {
            "path": "path/inside/bucket/file.tgz",
            "cluster_id": "3d59cd30-e2c3-4ec8-b35d-7df3b1feea2f",
            "sqs_message_id": "a_very_long_sqs_id",
        },
    ),
]


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_str(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = IDPConsumer(None, None, None, incoming_topic=None)
    assert sut.deserialize(KafkaMessage(msg)) == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_bytes(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = IDPConsumer(None, None, None, incoming_topic=None)
    assert sut.deserialize(KafkaMessage(msg.encode())) == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_bytearray(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = IDPConsumer(None, None, None, incoming_topic=None)
    assert sut.deserialize(KafkaMessage(bytearray(msg.encode()))) == value


_INVALID_MESSAGES = [
    "",
    '"path": "value"',
]


@pytest.mark.parametrize("msg", _INVALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_invalid_str(msg):
    """Test that invalid string JSON is not correctly deserialized."""
    sut = IDPConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(KafkaMessage(msg))


@pytest.mark.parametrize("msg, _", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_handles_valid(msg, _):
    """Test that `handles` method returns True for valid messages."""
    sut = IDPConsumer(None, None, None, incoming_topic=None)
    assert sut.handles(KafkaMessage(msg))


# This would have been a valid input, but it's supposed to be a `dict`, not `str`.
_DICT_STR = '{"path": "bucket/file"}'

_INVALID_RECORD_VALUES = [
    "",
    _DICT_STR.encode("utf-8"),
    bytearray(_DICT_STR.encode("utf-8")),
    [],
]

_VALID_RECORD_VALUES = [
    {"path": ""},
    {"path": "bucket/file"},
    {"path": "https://a-valid-domain.com/precious_url"},
]

_VALID_EMPTY_RECORD_VALUES = [
    {},
    {"notpath": "bucket/file"},
]


@pytest.mark.parametrize("value", _INVALID_RECORD_VALUES)
def test_get_url_invalid(value):
    """Test that `IDPConsumer.get_url` raises the appropriate exception."""
    with pytest.raises(Exception):
        IDPConsumer.get_url(None, value)


@pytest.mark.parametrize("value", _VALID_EMPTY_RECORD_VALUES)
def test_get_url_empty(value):
    """Test that `IDPConsumer.get_url` returns None."""
    assert IDPConsumer.get_url(None, value) is None


@pytest.mark.parametrize("value", _VALID_RECORD_VALUES)
def test_get_url_valid(value):
    """Test that `IDPConsumer.get_url` returns the expected value."""
    assert IDPConsumer.get_url(None, value) == value["path"]


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_create_broker():
    """Test that `create_broker` generates a broker with the expected values."""
    path = (
        "00000000/11111111-2222-3333-4444-555555555555/"
        "66666666666666-77777777777777777777777777777777"
    )
    input_msg = {
        "path": path,
    }

    sut = IDPConsumer(None, None, None, incoming_topic=None)
    broker = sut.create_broker(input_msg)

    assert broker["cluster_id"] == "11111111-2222-3333-4444-555555555555"
    assert broker["original_path"] == input_msg["path"]
    assert broker["org_id"] == "00000000"
    assert broker["year"] == "6666"
    assert broker["month"] == "66"
    assert broker["day"] == "66"
    assert broker["time"] == "666666"
    assert broker["id"] == "77777777777777777777777777777777"


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_create_broker_with_cluster_id_precedence():
    """Test that `create_broker` generates a broker with the expected values."""
    path = (
        "00000000/11111111-2222-3333-4444-555555555555/66666666666666-"
        "77777777777777777777777777777777"
    )
    input_msg = {
        "path": path,
        "cluster_id": "6a627474-05fc-41b1-8711-45ccc68238fe",
    }

    sut = IDPConsumer(None, None, None, incoming_topic=None)
    broker = sut.create_broker(input_msg)

    assert broker["cluster_id"] == input_msg["cluster_id"]
    assert broker["original_path"] == input_msg["path"]
    assert broker["org_id"] == "00000000"
    assert broker["year"] == "6666"
    assert broker["month"] == "66"
    assert broker["day"] == "66"
    assert broker["time"] == "666666"
    assert broker["id"] == "77777777777777777777777777777777"


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_create_broker_bad_path():
    """Test that `create_broker` raises an error when the path format doesn't match."""
    path = "11111111-2222-3333-4444-555555555555/66666666666666-77777777777777777777777777777777"

    input_msg = {
        "path": path,
        "cluster_id": "6a627474-05fc-41b1-8711-45ccc68238fe",
    }

    sut = IDPConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.create_broker(input_msg)


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_create_broker_bad_cluster_id_format():
    """Test that `create_broker` generates a broker with the expected values."""
    path = (
        "00000000/aaaaaaaa-bbbb-xxxx-ffff-badbadbadbad/"
        "66666666666666-77777777777777777777777777777777"
    )
    input_msg = {
        "path": path,
    }

    sut = IDPConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.create_broker(input_msg)


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_create_broker_bad_cluster_id_short():
    """Test that `create_broker` generates a broker with the expected values."""
    path = "00000000/aa-bbbb-cc-ffff-badbadbadbad/66666666666666-77777777777777777777777777777777"
    input_msg = {
        "path": path,
    }

    sut = IDPConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.create_broker(input_msg)


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
        sut = IDPConsumer(None, None, None, incoming_topic=None)
        input_msg = KafkaMessage("{}")

        frozen_time.move_to(t2)

        with patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process", lambda: None):
            sut.process_msg(input_msg)

        assert sut.last_received_message_time == t2.timestamp()
