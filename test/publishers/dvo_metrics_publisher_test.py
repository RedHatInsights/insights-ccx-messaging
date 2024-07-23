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

"""Tests for the DVOMetricsPublisher class."""

import json
from unittest.mock import MagicMock, patch

import freezegun
import pytest
from confluent_kafka import KafkaException

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.publishers.dvo_metrics_publisher import DVOMetricsPublisher


def test_init():
    """Check that init creates a valid object."""
    kakfa_config = {
        "bootstrap.servers": "kafka:9092",
    }
    DVOMetricsPublisher(outgoing_topic="topic name", **kakfa_config)


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
        DVOMetricsPublisher(topic_name)


INVALID_KWARGS = [
    {},
    {"bootstrap_servers": "kafka:9092"},
    {"bootstrap.servers": "kafka:9092", "unknown_option": "value"},
]


@pytest.mark.parametrize("kwargs", INVALID_KWARGS)
def test_bad_initialization(kwargs):
    """Check that init fails when using not valid kwargs."""
    with pytest.raises(KafkaException):
        DVOMetricsPublisher(outgoing_topic="topic", **kwargs)


@pytest.mark.parametrize("kafka_broker_cfg", INVALID_KWARGS)
@pytest.mark.parametrize("kwargs", INVALID_KWARGS)
def test_bad_init_with_kafka_config(kafka_broker_cfg, kwargs):
    """Check that init fails when using not valid kwargs."""
    with pytest.raises(KafkaException):
        DVOMetricsPublisher(outgoing_topic="topic", **kwargs)


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
    {"identity": {"identity": {"internal": {"org_id": 10}}}},  # missing timestamp
    {
        "identity": {
            "identity": {
                "internal": {"org_id": 10},
                "account_number": 1 + 2j,  # missing timestamp
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
    sut = DVOMetricsPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()

    with pytest.raises(CCXMessagingError):
        sut.publish(wrong_input_msg, {})
        assert not sut.producer.produce.called


VALID_INPUT_MSG = [
    pytest.param(
        {
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
        },
        {
            "OrgID": 10,
            "AccountNumber": 1,
            "ClusterName": "uuid",
            "Metrics": {"workload_recommendations": []},
            "RequestId": "a request id",
            "LastChecked": "a timestamp",
            "Version": 0,
        },
        id="with account",
    ),
    pytest.param(
        {
            "identity": {
                "identity": {
                    "internal": {"org_id": 10},
                    "account_number": 5 + 2j,
                },
            },
            "timestamp": "a timestamp",
            "cluster_name": "uuid",
            "request_id": "a request id",
            "topic": "incoming_topic",
            "partition": 0,
            "offset": 100,
        },
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Metrics": {"workload_recommendations": []},
            "RequestId": "a request id",
            "LastChecked": "a timestamp",
            "Version": 0,
        },
        id="invalid account",
    ),
    pytest.param(
        {
            "identity": {
                "identity": {
                    "internal": {"org_id": 10},
                    "account_number": "",
                },
            },
            "timestamp": "a timestamp",
            "cluster_name": "uuid",
            "request_id": "a request id",
            "topic": "incoming_topic",
            "partition": 0,
            "offset": 100,
        },
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Metrics": {"workload_recommendations": []},
            "RequestId": "a request id",
            "LastChecked": "a timestamp",
            "Version": 0,
        },
        id="empty account",
    ),
    pytest.param(
        {
            "identity": {
                "identity": {
                    "internal": {"org_id": 10},
                },
            },
            "timestamp": "a timestamp",
            "cluster_name": "uuid",
            "request_id": "a request id",
            "topic": "incoming_topic",
            "partition": 0,
            "offset": 100,
        },
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Metrics": {"workload_recommendations": []},
            "RequestId": "a request id",
            "LastChecked": "a timestamp",
            "Version": 0,
        },
        id="no account",
    ),
    pytest.param(
        {
            "identity": {
                "identity": {
                    "internal": {"org_id": 10},
                },
            },
            "timestamp": "a timestamp",
            "cluster_name": "uuid",
            "request_id": "a request id",
            "topic": "incoming_topic",
            "partition": 0,
            "offset": 100,
            "metadata": {"custom_metadata": {"gathering_time": "2023-08-14T09:31:46Z"}},
        },
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Metrics": {"workload_recommendations": []},
            "RequestId": "a request id",
            "LastChecked": "a timestamp",
            "Version": 0,
        },
        id="with gathering timestamp",
    ),
    pytest.param(
        {
            "identity": {
                "identity": {
                    "internal": {"org_id": 10},
                },
            },
            "timestamp": "a timestamp",
            "cluster_name": "uuid",
            "request_id": "a request id",
            "topic": "incoming_topic",
            "partition": 0,
            "offset": 100,
            "metadata": {"custom_metadata": {"gathering_time": "2023-08-14T09:31:46.677052"}},
        },
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Metrics": {"workload_recommendations": []},
            "RequestId": "a request id",
            "LastChecked": "a timestamp",
            "Version": 0,
        },
        id="with gathering timestamp in ISO format",
    ),
    pytest.param(
        {
            "identity": {
                "identity": {
                    "internal": {"org_id": 10},
                },
            },
            "timestamp": "a timestamp",
            "cluster_name": "uuid",
            "request_id": "a request id",
            "topic": "incoming_topic",
            "partition": 0,
            "offset": 100,
            "metadata": {"custom_metadata": {"some_timestamp": "2023-08-14T09:31:46.677052"}},
        },
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Metrics": {"workload_recommendations": []},
            "RequestId": "a request id",
            "LastChecked": "a timestamp",
            "Version": 0,
        },
        id="with custom metadata without gathering timestamp",
    ),
    pytest.param(
        {
            "identity": {
                "identity": {
                    "internal": {"org_id": 10},
                },
            },
            "timestamp": "a timestamp",
            "cluster_name": "uuid",
            "request_id": "a request id",
            "topic": "incoming_topic",
            "partition": 0,
            "offset": 100,
            "metadata": {},
        },
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Metrics": {"workload_recommendations": []},
            "RequestId": "a request id",
            "LastChecked": "a timestamp",
            "Version": 0,
        },
        id="empty metadata",
    ),
]


@freezegun.freeze_time("2012-01-14")
@pytest.mark.parametrize("input, expected_output", VALID_INPUT_MSG)
def test_publish_valid(input, expected_output):
    """Check that Kafka producer is called with an expected message."""
    report = '{"workload_recommendations": []}'

    expected_output = json.dumps(expected_output) + "\n"
    sut = DVOMetricsPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
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
    sut = DVOMetricsPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()

    with pytest.raises(CCXMessagingError):
        sut.publish(VALID_INPUT_MSG, invalid_report)
        assert not sut.producer.produce.called


@freezegun.freeze_time("2012-01-14")
@pytest.mark.parametrize("input,output", VALID_INPUT_MSG)
def test_error(input, output):
    """Check that error just prints a log."""
    _ = output  # output values are not needed

    sut = DVOMetricsPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})

    with patch("ccx_messaging.publishers.kafka_publisher.log") as log_mock:
        sut.error(input, None)
        assert log_mock.warning.called


VALID_REPORTS = [
    pytest.param(
        json.dumps(
            {"version": 1, "reports": [], "pass": [], "info": [], "workload_recommendations": []}
        ),
        {
            "OrgID": 10,
            "AccountNumber": 1,
            "ClusterName": "uuid",
            "Metrics": {"pass": [], "info": [], "workload_recommendations": []},
            "RequestId": "a request id",
            "LastChecked": "a timestamp",
            "Version": 1,
        },
        id="valid_report",
    )
]


@pytest.mark.parametrize("input,expected_output", VALID_REPORTS)
def test_filter_dvo_results(input, expected_output):
    """Check that the external rule reports are filtered out from the engine results."""
    sut = DVOMetricsPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()
    expected_output = json.dumps(expected_output) + "\n"

    sut.publish(VALID_INPUT_MSG[0][0][0], input)
    sut.producer.produce.assert_called_with("outgoing_topic", expected_output.encode())


def test_empty_dvo_results():
    """Check that the publisher does not send empty message."""
    sut = DVOMetricsPublisher("outgoing_topic", {"bootstrap.servers": "kafka:9092"})
    sut.producer = MagicMock()

    input = json.dumps({"version": 1, "reports": [], "pass": [], "info": []})
    sut.publish(VALID_INPUT_MSG[0][0][0], input)
    assert not sut.producer.produce.called
