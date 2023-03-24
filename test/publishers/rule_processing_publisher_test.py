# Copyright 2023 Red Hat, Inc
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

"""Tests for the RuleProcessingPublisher class."""

import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.publishers.rule_processing_publisher import RuleProcessingPublisher


def test_init():
    """Check that init creates a valid object."""
    kakfa_config = {
        "bootstrap.servers": "kafka:9092",
    }
    RuleProcessingPublisher(outgoing_topic="topic name", **kakfa_config)


INVALID_TOPIC_NAMES = [
    None,
    b"Topic name",
    [],
    {},
    4,
    5.5,
    5 + 2j,
]


@pytest.mark.parametrize("topic_name", INVALID_TOPIC_NAMES)
def test_init_invalid_topic(topic_name):
    """Check what happens when the output_topic parameter is not valid."""
    with pytest.raises(CCXMessagingError):
        RuleProcessingPublisher(topic_name)


INVALID_KWARGS = [
    {},
    {"bootstrap_servers": "kafka:9092"},
    {"bootstrap.servers": "kafka:9092", "unknown_option": "value"},
]


@pytest.mark.parametrize("kwargs", INVALID_KWARGS)
def test_bad_initialization(kwargs):
    """Check that init fails when using not valid kwargs."""
    with pytest.raises(KafkaException):
        RuleProcessingPublisher(outgoing_topic="topic", **kwargs)


@pytest.mark.parametrize("kafka_broker_cfg", INVALID_KWARGS)
@pytest.mark.parametrize("kwargs", INVALID_KWARGS)
def test_bad_init_with_kafka_config(kafka_broker_cfg, kwargs):
    """Check that init fails when using not valid kwargs."""
    with pytest.raises(KafkaException):
        RuleProcessingPublisher(outgoing_topic="topic", **kwargs)


INVALID_INPUT_MSGS = [
    None,
    "",
    1,
    2.0,
    3 + 1j,
    [],
    {},  # right type, missing identity
    {"identity": {}},  # missing identity-identity
    {"identity": {"identity": {}}},  # missing identity-identity-internal
    {"identity": {"identity": {"internal": {}}}},  # missing identity-identity-internal-org_id
    {"identity": {"identity": {"internal": {"org_id": 15.2}}}},  # incorrect org_id type
    {"identity": {"identity": {"internal": {"org_id": 10}}}},  # missing "account_number"
    {
        "identity": {
            "identity": {
                "internal": {"org_id": 10},
                "account_number": 1 + 2j,  # incorrect account number type
            },
        },
    },
    {
        "identity": {
            "identity": {
                "internal": {"org_id": 10},
                "account_number": 1,
            },
        },
    },  # missing timestamp
    {
        "identity": {
            "identity": {
                "internal": {"org_id": 10},
                "account_number": 1,
            },
        },
        "timestamp": "a timestamp",
    },  # missing cluster_name
]


@pytest.mark.parametrize("wrong_input_msg", INVALID_INPUT_MSGS)
def test_publish_bad_argument(wrong_input_msg):
    """Check that invalid messages passed by the framework are handled gracefully."""
    sut = RuleProcessingPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()

    with pytest.raises(CCXMessagingError):
        sut.publish(wrong_input_msg, {})
        assert not sut.producer.produce.called


VALID_INPUT_MSG = {
    "identity": {
        "identity": {
            "internal": {"org_id": 10},
            "account_number": 1,
        },
    },
    "timestamp": "a timestamp",
    "cluster_name": "uuid",
    "request_id": "a request id",
    "topic": "incoming_topic",
    "partition": 0,
    "offset": 100,
}


def test_publish_valid():
    """Check that Kafka producer is called with an expected message."""
    report = "{}"

    expected_output = (
        json.dumps(
            {
                "OrgID": 10,
                "AccountNumber": 1,
                "ClusterName": "uuid",
                "Report": {},
                "LastChecked": "a timestamp",
                "Version": 2,
                "RequestId": "a request id",
            }
        )
        + "\n"
    )
    sut = RuleProcessingPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()

    sut.publish(VALID_INPUT_MSG, report)
    sut.producer.produce.assert_called_with("outgoing_topic", expected_output.encode())


INVALID_REPORTS = [
    None,
    1,
    2.0,
    1 + 3j,
    [],
    {},
    "",
]


@pytest.mark.parametrize("invalid_report", INVALID_REPORTS)
def test_publish_invalid_report(invalid_report):
    """Check the behaviour of publish when an invalid report is received."""
    sut = RuleProcessingPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()

    with pytest.raises(CCXMessagingError):
        sut.publish(VALID_INPUT_MSG, invalid_report)
        assert not sut.producer.produce.called


def test_error():
    """Check that error just prints a log."""
    sut = RuleProcessingPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})

    with patch("ccx_messaging.publishers.rule_processing_publisher.LOG") as log_mock:
        sut.error(VALID_INPUT_MSG, None)
        assert log_mock.error.called
