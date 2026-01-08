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

"""Tests for the RuleProcessingPublisher class."""

import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.publishers.idp_rule_processing_publisher import IDPRuleProcessingPublisher


def test_init():
    """Check that init creates a valid object."""
    kakfa_config = {
        "bootstrap.servers": "kafka:9092",
    }
    IDPRuleProcessingPublisher(outgoing_topic="topic name", **kakfa_config)


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
        IDPRuleProcessingPublisher(topic_name)


INVALID_KWARGS = [
    {},
    {"bootstrap_servers": "kafka:9092"},
    {"bootstrap.servers": "kafka:9092", "unknown_option": "value"},
]


@pytest.mark.parametrize("kwargs", INVALID_KWARGS)
def test_bad_initialization(kwargs):
    """Check that init fails when using not valid kwargs."""
    with pytest.raises(KafkaException):
        IDPRuleProcessingPublisher(outgoing_topic="topic", **kwargs)


@pytest.mark.parametrize("kafka_broker_cfg", INVALID_KWARGS)
@pytest.mark.parametrize("kwargs", INVALID_KWARGS)
def test_bad_init_with_kafka_config(kafka_broker_cfg, kwargs):
    """Check that init fails when using not valid kwargs."""
    with pytest.raises(KafkaException):
        IDPRuleProcessingPublisher(outgoing_topic="topic", **kwargs)


INVALID_INPUT_MSGS = [
    None,
    "",
    1,
    2.0,
    3 + 1j,
    [],
    {},  # right type, missing path and metadata
    {"path": ""},  # missing metadata
    {"path": "", "metadata": {}},  # missing metadata-cluster_id
]


@pytest.mark.parametrize("wrong_input_msg", INVALID_INPUT_MSGS)
def test_publish_bad_argument(wrong_input_msg):
    """Check that invalid messages passed by the framework are handled gracefully."""
    sut = IDPRuleProcessingPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()

    with pytest.raises(CCXMessagingError):
        sut.publish(wrong_input_msg, {})
        assert not sut.producer.produce.called


VALID_INPUT_MSG = [
    pytest.param(
        {
            "path": "bucket/path/to/archive.tgz",
            "metadata": {
                "cluster_id": "uuid",
            },
            "request_id": "ingress-service-7777777777-q4444/4444444444-000001",
        },
        {
            "path": "bucket/path/to/archive.tgz",
            "metadata": {
                "cluster_id": "uuid",
            },
            "report": {
                "reports": [],
            },
            "request_id": "ingress-service-7777777777-q4444/4444444444-000001",
        },
        id="minimal valid",
    ),
    pytest.param(
        {
            "path": "bucket/path/to/archive.tgz",
            "original_path": "other_than_current_path",
            "metadata": {"cluster_id": "uuid", "external_organization": "an organization"},
            "request_id": "ingress-service-7777777777-q4444/4444444444-000001",
        },
        {
            "path": "bucket/path/to/archive.tgz",
            "metadata": {"cluster_id": "uuid", "external_organization": "an organization"},
            "report": {
                "reports": [],
            },
            "request_id": "ingress-service-7777777777-q4444/4444444444-000001",
        },
        id="adding optional elements",
    ),
]


@pytest.mark.parametrize("input, expected_output", VALID_INPUT_MSG)
def test_publish_valid(input, expected_output):
    """Check that Kafka producer is called with an expected message."""
    report = '{"reports": []}'

    expected_output = json.dumps(expected_output)
    sut = IDPRuleProcessingPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()

    sut.publish(input, report)
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
    sut = IDPRuleProcessingPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()

    with pytest.raises(CCXMessagingError):
        sut.publish(VALID_INPUT_MSG, invalid_report)
        assert not sut.producer.produce.called


@pytest.mark.parametrize("input,output", VALID_INPUT_MSG)
def test_error(input, output):
    """Check that error just prints a log."""
    _ = output  # output values are not needed

    sut = IDPRuleProcessingPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})

    with patch("ccx_messaging.publishers.kafka_publisher.log") as log_mock:
        sut.error(input, None)
        assert log_mock.warning.called
