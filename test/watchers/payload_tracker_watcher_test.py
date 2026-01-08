# Copyright 2020 Red Hat, Inc
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

"""Module containing unit tests for the `ConsumerWatcher` class."""

from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from ccx_messaging.watchers.payload_tracker_watcher import PayloadTrackerWatcher


_INVALID_SERVERS = [
    None,
    [],
    {},
    100,
    100.5,
    "kafka_instance",
]

_INVALID_TOPICS = [
    None,
    [],
    {},
    "",
]


def _prepare_kafka_mock(producer_init_mock):
    """Create a producer mock from its mocked initialization."""
    producer_mock = MagicMock()
    producer_init_mock.return_value = producer_mock
    return producer_mock


@pytest.mark.parametrize("bootstrap_value", _INVALID_SERVERS)
@pytest.mark.parametrize("topic_value", _INVALID_TOPICS)
def test_payload_tracker_watcher_invalid_initialize_invalid_servers(bootstrap_value, topic_value):
    """Test passing invalid data types or values to the `PayloadTrackerWatcher` initializer."""
    with pytest.raises((TypeError, PermissionError, OverflowError, KeyError)):
        _ = PayloadTrackerWatcher(topic_value, **{"bootstrap.servers": bootstrap_value})


@patch("ccx_messaging.watchers.payload_tracker_watcher.Producer")
def test_payload_tracker_init_with_kafka_config(producer_init_mock):
    """Check that passing a kafka_broker_config parameter updates the default ones."""
    kafka_broker_cfg = {"bootstrap.servers": "valid_server"}

    PayloadTrackerWatcher(
        "valid_topic",
        kafka_broker_config=kafka_broker_cfg,
        **{"bootstrap.servers": "invalid_servicer"},
    )

    producer_init_mock.assert_called_with(**kafka_broker_cfg)


@freeze_time("2020-05-07T14:00:00")
@patch("ccx_messaging.watchers.payload_tracker_watcher.Producer")
def test_payload_tracker_watcher_publish_status(producer_init_mock):
    """Test publish_status method sends the expected value to Kafka."""
    mocked_values = {
        "request_id": "some request id",
        "identity": {
            "identity": {
                "internal": {
                    "org_id": 1,
                },
                "account_number": 2,
            }
        },
    }

    producer_mock = _prepare_kafka_mock(producer_init_mock)
    sut = PayloadTrackerWatcher("valid_topic", **{"bootstrap.servers": "bootstrap_server"})
    sut.on_recv(mocked_values)
    producer_mock.produce.assert_called_with(
        "valid_topic",
        b'{"service": "ccx-data-pipeline", "request_id": "some request id", '
        b'"status": "received", "date": "2020-05-07T14:00:00", '
        b'"org_id": 1, "account": 2}',
    )

    sut.on_process(mocked_values, "{result}")
    producer_mock.produce.assert_called_with(
        "valid_topic",
        b'{"service": "ccx-data-pipeline", "request_id": "some request id", '
        b'"status": "processing", "date": "2020-05-07T14:00:00", '
        b'"org_id": 1, "account": 2}',
    )

    sut.on_consumer_success(mocked_values, "broker", "{result}")
    producer_mock.produce.assert_called_with(
        "valid_topic",
        b'{"service": "ccx-data-pipeline", "request_id": "some request id", '
        b'"status": "success", "date": "2020-05-07T14:00:00", '
        b'"org_id": 1, "account": 2}',
    )

    sut.on_consumer_failure(mocked_values, Exception("Something"))
    producer_mock.produce.assert_called_with(
        "valid_topic",
        b'{"service": "ccx-data-pipeline", "request_id": "some request id", '
        b'"status": "error", "date": "2020-05-07T14:00:00", '
        b'"org_id": 1, "account": 2, "status_msg": "Something"}',
    )

    # call _publish_status without request_id and check there is not any
    # exception thrown
    del mocked_values["request_id"]
    sut.on_recv(mocked_values)
