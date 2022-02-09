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

"""Module for testing the ccx_data_pipeline.kafka_publisher module."""

import unittest
from unittest.mock import MagicMock, patch

from kafka.consumer.fetcher import ConsumerRecord

from ccx_messaging.publishers.data_pipeline_publisher import DataPipelinePublisher
from ccx_messaging.error import CCXMessagingError


def _mock_consumer_record(value):
    """Construct a value-only `ConsumerRecord`."""
    return ConsumerRecord(None, None, None, None, None, None, value, None, None, None, None, None)


class DataPipelinePublisherTest(unittest.TestCase):
    """Test cases for testing the class DataPipelinePublisher."""

    def test_init(self):
        """
        Test DataPipelinePublisher initializer.

        The test mocks the KafkaProducer from kafka module in order
        to avoid real usage of the library
        """
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "outgoing_topic": "a topic name",
            "client_id": "ccx-data-pipeline",
        }

        with patch("ccx_messaging.publishers.data_pipeline_publisher.KafkaProducer") as kafka_producer_mock:
            sut = DataPipelinePublisher(**producer_kwargs)

            kafka_producer_mock.assert_called_with(
                bootstrap_servers=["kafka_server1"], client_id="ccx-data-pipeline"
            )
            self.assertEqual(sut.topic, "a topic name")

    def test_init_no_topic(self):
        """Test DataPipelinePublisher initializer without outgoing topic."""
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        with self.assertRaises(TypeError):
            _ = DataPipelinePublisher(**producer_kwargs)

    # pylint: disable=no-self-use
    def test_publish_no_request_id(self):
        """
        Test Producer.publish method without request_id field.

        The kafka.KafkaProducer class is mocked in order to avoid the usage
        of the real library
        """
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        topic_name = "KAFKATOPIC"
        values = {
            "ClusterName": "the cluster name",
            "identity": {"identity": {"account_number": "3000", "internal": {"org_id": "5000"}}},
            "timestamp": "2020-01-23T16:15:59.478901889Z",
        }
        input_msg = _mock_consumer_record(values)
        message_to_publish = '{"key1": "value1"}'
        expected_message = (
            b'{"OrgID": 5000, "AccountNumber": 3000, "ClusterName": "the cluster name", '
            b'"Report": {"key1": "value1"}, "LastChecked": "2020-01-23T16:15:59.478901889Z", '
            b'"Version": 2, "RequestId": null}\n'
        )

        with patch("ccx_messaging.publishers.data_pipeline_publisher.KafkaProducer") as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = DataPipelinePublisher(outgoing_topic=topic_name, **producer_kwargs)

            sut.publish(input_msg, message_to_publish)
            producer_mock.send.assert_called_with(topic_name, expected_message)

    def test_publish(self):
        """
        Test Producer.publish method.

        The kafka.KafkaProducer class is mocked in order to avoid the usage
        of the real library
        """
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        topic_name = "KAFKATOPIC"
        values = {
            "ClusterName": "the cluster name",
            "identity": {"identity": {"account_number": "3000", "internal": {"org_id": "5000"}}},
            "timestamp": "2020-01-23T16:15:59.478901889Z",
            "request_id": "REQUEST_ID",
        }
        input_msg = _mock_consumer_record(values)
        message_to_publish = '{"key1": "value1"}'
        expected_message = (
            b'{"OrgID": 5000, "AccountNumber": 3000, "ClusterName": "the cluster name", '
            b'"Report": {"key1": "value1"}, "LastChecked": "2020-01-23T16:15:59.478901889Z", '
            b'"Version": 2, "RequestId": "REQUEST_ID"}\n'
        )

        with patch("ccx_messaging.publishers.data_pipeline_publisher.KafkaProducer") as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = DataPipelinePublisher(outgoing_topic=topic_name, **producer_kwargs)

            sut.publish(input_msg, message_to_publish)
            producer_mock.send.assert_called_with(topic_name, expected_message)

    def test_publish_bad_orgID(self):
        """
        Test Producer.publish method with invalid orgID.

        The kafka.KafkaProducer class is mocked in order to avoid the usage
        of the real library
        """
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        topic_name = "KAFKATOPIC"
        values = {
            "ClusterName": "the cluster name",
            "identity": {"identity": {"account_number": "3000", "internal": {"org_id": "NaN"}}},
            "timestamp": "2020-01-23T16:15:59.478901889Z",
        }
        input_msg = _mock_consumer_record(values)
        message_to_publish = '{"key1": "value1"}'

        with patch("ccx_messaging.publishers.data_pipeline_publisher.KafkaProducer") as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = DataPipelinePublisher(outgoing_topic=topic_name, **producer_kwargs)

            with self.assertRaises(CCXMessagingError):
                sut.publish(input_msg, message_to_publish)

    def test_publish_bad_accountNumber(self):
        """
        Test Producer.publish method with invalid orgID.

        The kafka.KafkaProducer class is mocked in order to avoid the usage
        of the real library
        """
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        topic_name = "KAFKATOPIC"
        values = {
            "ClusterName": "the cluster name",
            "identity": {"identity": {"account_number": "NaN", "internal": {"org_id": "5000"}}},
            "timestamp": "2020-01-23T16:15:59.478901889Z",
        }
        input_msg = _mock_consumer_record(values)
        message_to_publish = '{"key1": "value1"}'

        with patch("ccx_messaging.publishers.data_pipeline_publisher.KafkaProducer") as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = DataPipelinePublisher(outgoing_topic=topic_name, **producer_kwargs)

            with self.assertRaises(CCXMessagingError):
                sut.publish(input_msg, message_to_publish)
