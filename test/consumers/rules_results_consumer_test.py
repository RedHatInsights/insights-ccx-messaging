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

"""Module for testing `RulesResultsConsumer` class."""

import json
import os
import pytest
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaException

from ccx_messaging.consumers.rules_results_consumer import RulesResultsConsumer
from ccx_messaging.error import CCXMessagingError


from . import KafkaMessage


_VALID_MESSAGE_CONTENT = {
    "path": "archives/compressed/aa/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/202101/20/031044.tar.gz",
    "metadata": {
        "cluster_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    },
    "report": {
        "system": {},
        "reports": [],
        "skips": [],
        "pass": [],
        "info": [],
    },
}

_INVALID_TYPE_VALUES = [
    None,
    42,
    3.14,
    True,
    [],
    {},
]


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_get_url():
    """Test that `get_url` method retrieves path from correct field."""
    consumer = RulesResultsConsumer(None, None, None, incoming_topic=None)
    msg = {"file_path": "test"}
    assert consumer.get_url(msg) == "test"


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_get_url_invalid():
    """Test that `get_url` method raises exception if `file_path` field is missing."""
    consumer = RulesResultsConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(Exception):
        consumer.get_url({})


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_msg():
    """Test that valid message is correctly deserialized."""
    consumer = RulesResultsConsumer(None, None, None, incoming_topic=None)
    msg = consumer.deserialize(KafkaMessage(json.dumps(_VALID_MESSAGE_CONTENT)))
    assert msg == _VALID_MESSAGE_CONTENT


@pytest.mark.parametrize("value", _INVALID_TYPE_VALUES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_invalid_type(mock_consumer, value):
    """Test that passing invalid data type to `deserialize` raises an exception."""
    sut = RulesResultsConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(value)


@pytest.mark.parametrize("value", _INVALID_TYPE_VALUES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_invalid_type_as_kafka_message(mock_consumer, value):
    """Test that passing invalid data type to `deserialize` raises an exception."""
    sut = RulesResultsConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(KafkaMessage(value))


_INVALID_MESSAGES = [
    {
        "metadata": {
            "cluster_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        },
        "report": {
            "system": {},
            "reports": [],
            "skips": [],
            "pass": [],
            "info": [],
        },
    },
    {
        "path": "archives/compressed/aa/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/202101/20/031044.tar.gz",  # noqa: E501
        "report": {
            "system": {},
            "reports": [],
            "skips": [],
            "pass": [],
            "info": [],
        },
    },
    {
        "path": "archives/compressed/aa/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/202101/20/031044.tar.gz",  # noqa: E501
        "metadata": {},
        "report": {
            "system": {},
            "reports": [],
            "skips": [],
            "pass": [],
            "info": [],
        },
    },
    {
        "path": "archives/compressed/aa/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/202101/20/031044.tar.gz",  # noqa: E501
        "metadata": {
            "cluster_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        },
    },
]


@pytest.mark.parametrize("value", _INVALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_invalid_msg_format(value):
    """Test that error is raised when deserializing messages with invalid format."""
    sut = RulesResultsConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(KafkaMessage(json.dumps(value)))


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_process_msg_temp_file():
    """Test that the temporary file is created and removed during the message processing."""
    file_name = ""

    def test_file_exists(_, msg):
        nonlocal file_name
        assert "file_path" in msg
        file_name = msg["file_path"]
        assert os.path.exists(file_name)

    consumer = RulesResultsConsumer(None, None, None, incoming_topic=None)
    with patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process", test_file_exists):
        consumer.process_msg(KafkaMessage(json.dumps(_VALID_MESSAGE_CONTENT)))
    assert not os.path.exists(file_name)


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_process_msg_raise_error():
    """Test when the `process` message raises an error."""
    consumer = RulesResultsConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(KafkaException):
        consumer.process_msg(KafkaMessage(error=True))


@pytest.mark.parametrize("value", _INVALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_process_msg_invalid_format(value):
    """Test when the `process` message raises an error."""
    consumer = RulesResultsConsumer(None, None, None, incoming_topic=None)
    with patch(
        "ccx_messaging.consumers.rules_results_consumer.RulesResultsConsumer.process_dead_letter",
    ) as dlq_mock:
        consumer.process_msg(KafkaMessage(json.dumps(value)))
        assert dlq_mock.called


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_create_broker():
    """Test that broker contains all expected fields."""
    consumer = RulesResultsConsumer(None, None, None, incoming_topic=None)
    with patch(
        "ccx_messaging.consumers.rules_results_consumer.RulesResultsConsumer.create_report_path",
    ) as create_path_mock:
        create_path_mock.return_value = "mock_path"
        broker = consumer.create_broker(_VALID_MESSAGE_CONTENT)
        assert create_path_mock.called
        assert "report_path" in broker.keys()
        assert "cluster_id" in broker.keys()
        assert broker["report_path"] == "mock_path"
        assert broker["cluster_id"] == _VALID_MESSAGE_CONTENT["metadata"]["cluster_id"]


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_create_report_path():
    """Test that the report path is correctly created."""
    consumer = RulesResultsConsumer(None, None, None, incoming_topic=None)
    path = consumer.create_report_path(_VALID_MESSAGE_CONTENT)
    assert path == "insights/aa/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/202101/20/031044/insights.json"
