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

from confluent_kafka import KafkaException

from ccx_messaging.consumers.synced_archive_consumer import SyncedArchiveConsumer
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
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(value)


@pytest.mark.parametrize("value", _INVALID_TYPE_VALUES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_invalid_type_as_kafka_message(mock_consumer, value):
    """Test that passing invalid data type to `deserialize` raises an exception."""
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(KafkaMessage(value))


_VALID_MESSAGES = [
    (
        '{"path": "", "metadata": {"cluster_id": ""}}',
        {
            "path": "",
            "metadata": {
                "cluster_id": "",
            },
        },
    ),
    (
        '{"path": "path/to/archive.tgz", "metadata": {"cluster_id": ""}}',
        {
            "path": "path/to/archive.tgz",
            "metadata": {
                "cluster_id": "",
            },
        },
    ),
    (
        '{"path": "path/to/archive.tgz", "metadata": {"cluster_id": "a_cluster_name"},'
        '"original_path": ""}',
        {
            "path": "path/to/archive.tgz",
            "metadata": {
                "cluster_id": "a_cluster_name",
            },
            "original_path": "",
        },
    ),
    (
        '{"path": "path/to/archive.tgz", "original_path": "other/path/archive.tgz", '
        '"metadata": {"cluster_id": "a_cluster_name", "external_organization": ""}}',
        {
            "path": "path/to/archive.tgz",
            "metadata": {
                "cluster_id": "a_cluster_name",
                "external_organization": "",
            },
            "original_path": "other/path/archive.tgz",
        },
    ),
]


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_str(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
    assert sut.deserialize(KafkaMessage(msg)) == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_bytes(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
    assert sut.deserialize(KafkaMessage(msg.encode())) == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_bytearray(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
    assert sut.deserialize(KafkaMessage(bytearray(msg.encode()))) == value


_INVALID_MESSAGES = [
    "",
    '"path": "value"',
]


@pytest.mark.parametrize("msg", _INVALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_invalid_str(msg):
    """Test that invalid string JSON is not correctly deserialized."""
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(KafkaMessage(msg))


@pytest.mark.parametrize("msg, _", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_handles_valid(msg, _):
    """Test that `handles` method returns True for valid messages."""
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
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


@pytest.mark.parametrize("value", _INVALID_RECORD_VALUES)
def test_get_url_invalid(value):
    """Test that `SyncedArchiveConsumer.get_url` raises the appropriate exception."""
    with pytest.raises(Exception):
        SyncedArchiveConsumer.get_url(None, value)


@pytest.mark.parametrize("value", _VALID_RECORD_VALUES)
def test_get_url_valid(value):
    """Test that `SyncedArchiveConsumer.get_url` returns the expected value."""
    assert SyncedArchiveConsumer.get_url(None, value) == value["path"]


def test_create_broker():
    """Test that `SyncedArchiveConsumer.create_broker` doesn't report any error."""
    assert SyncedArchiveConsumer.create_broker(None, {}) is not None


_VALID_KAFKA_MESSAGES = [KafkaMessage(value) for value in _VALID_RECORD_VALUES]
_VALID_KAFKA_MESSAGES.extend(
    [
        None,
    ]
)


@pytest.mark.parametrize("value", _VALID_KAFKA_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_process_msg(value: dict[str, str]):
    """Test right `process_msg` behaviour."""
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
    with patch(
        "ccx_messaging.consumers.synced_archive_consumer.SyncedArchiveConsumer.process",
        lambda: None,
    ):
        sut.process_msg(value)


_INVALID_KAFKA_MESSAGES = [KafkaMessage(value) for value in _INVALID_RECORD_VALUES]


@pytest.mark.parametrize("value", _INVALID_KAFKA_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_process_invalid_msg(value: dict[str, str]):
    """Test right `process_msg` behaviour."""
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
    with patch(
        "ccx_messaging.consumers.synced_archive_consumer.SyncedArchiveConsumer.process_dead_letter",
    ) as dlq_mock:
        sut.process_msg(value)
        assert dlq_mock.called


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_process_msg_raise_error():
    """Test when the `process` message raises an error."""
    sut = SyncedArchiveConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(KafkaException):
        sut.process_msg(KafkaMessage(error=True))


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
        sut = SyncedArchiveConsumer(None, None, None, None)
        input_msg = KafkaMessage("{}")

        frozen_time.move_to(t2)

        with patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process", lambda: None):
            sut.process_msg(input_msg)

        assert sut.last_received_message_time == t2.timestamp()
