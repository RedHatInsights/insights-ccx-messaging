# Copyright 2022 Red Hat, Inc
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

"""Module for testing the ccx-messaging.sha_publisher module."""

import unittest
from unittest.mock import MagicMock, patch

from ccx_messaging.publishers.sha_publisher import SHAPublisher
from ccx_messaging.error import CCXMessagingError

from .unicode_encode_error_thower import UnicodeEncodeErrorThrower


input_msg = {
    "topic": "topic name",
    "partition": "partition name",
    "offset": 1234,
    "url": "any/url",
    "identity": {
        "identity": {
            "internal": {
                "org_id": "12345678",
            },
            "account_number": "999999",
        },
    },
    "timestamp": "2020-01-23T16:15:59.478901889Z",
    "cluster_name": "clusterName",
}


class SHAPublisherTest(unittest.TestCase):
    """Test cases for testing the class SHAPublisher."""

    def test_init(self):
        """
        Test SHAPublisher initializer.

        The test mocks the KafkaProducer from kafka module in order
        to avoid real usage of the library
        """
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "outgoing_topic": "a topic name",
            "client_id": "ccx-data-pipeline",
        }

        with patch(
            "ccx_messaging.publishers.sha_publisher.KafkaProducer"
        ) as kafka_producer_mock:
            sut = SHAPublisher(**producer_kwargs)

            kafka_producer_mock.assert_called_with(
                bootstrap_servers=["kafka_server1"], client_id="ccx-data-pipeline"
            )
            self.assertEqual(sut.topic, "a topic name")

    def test_init_improper_params(self):
        """Test SHAPublisher initializer without improper parameters."""
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        with self.assertRaises(TypeError):
            _ = SHAPublisher(**producer_kwargs)

    def test_init_none_topic(self):
        """Test SHAPublisher initializer without outgoing topic."""
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        with self.assertRaises(KeyError):
            _ = SHAPublisher(outgoing_topic=None, **producer_kwargs)

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
        message_to_publish = '{"key1": "value1"}'
        expected_message = (
            b'{"OrgID": 12345678, "AccountNumber": 999999, "ClusterName": "clusterName", '
            b'"Images": {"key1": "value1"}, "LastChecked": "2020-01-23T16:15:59.478901889Z", '
            b'"Version": 2, "RequestId": null}\n'
        )

        with patch(
            "ccx_messaging.publishers.sha_publisher.KafkaProducer"
        ) as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = SHAPublisher(outgoing_topic=topic_name, **producer_kwargs)

            sut.publish(input_msg, message_to_publish)
            producer_mock.send.assert_called_with(topic_name, expected_message)

    def test_publish_wrong_input_message(self):
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
        message_to_publish = ""

        with patch(
            "ccx_messaging.publishers.sha_publisher.KafkaProducer"
        ) as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = SHAPublisher(outgoing_topic=topic_name, **producer_kwargs)

            with self.assertRaises(Exception):
                sut.publish(input_msg, message_to_publish)

    def test_publish_wrong_org_id(self):
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
        message_to_publish = ""

        input_msg = {
            "topic": "topic name",
            "partition": "partition name",
            "offset": 1234,
            "url": "any/url",
            "identity": {
                "identity": {
                    "internal": {
                        "org_id": "*** not an integer ***",
                    },
                    "account_number": "999999",
                },
            },
            "timestamp": "2020-01-23T16:15:59.478901889Z",
            "cluster_name": "clusterName",
        }

        with patch(
            "ccx_messaging.publishers.sha_publisher.KafkaProducer"
        ) as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = SHAPublisher(outgoing_topic=topic_name, **producer_kwargs)

            with self.assertRaises(CCXMessagingError):
                sut.publish(input_msg, message_to_publish)

    def test_publish_wrong_account_number(self):
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
        message_to_publish = ""

        input_msg = {
            "topic": "topic name",
            "partition": "partition name",
            "offset": 1234,
            "url": "any/url",
            "identity": {
                "identity": {
                    "internal": {
                        "org_id": "123456",
                    },
                    "account_number": "*** not an integer ***",
                },
            },
            "timestamp": "2020-01-23T16:15:59.478901889Z",
            "cluster_name": "clusterName",
        }

        with patch(
            "ccx_messaging.publishers.sha_publisher.KafkaProducer"
        ) as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = SHAPublisher(outgoing_topic=topic_name, **producer_kwargs)

            with self.assertRaises(CCXMessagingError):
                sut.publish(input_msg, message_to_publish)

    def test_error(self):
        """
        Test Producer.error() method.
        """
        err = CCXMessagingError("foobar")

        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        topic_name = "KAFKATOPIC"

        with patch(
            "ccx_messaging.publishers.sha_publisher.KafkaProducer"
        ) as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = SHAPublisher(outgoing_topic=topic_name, **producer_kwargs)

            err = CCXMessagingError("foobar")

            sut.error(input_msg, err)

    def test_error_wrong_type(self):
        """
        Test Producer.error() method.
        """
        err = CCXMessagingError("foobar")

        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        topic_name = "KAFKATOPIC"

        with patch(
            "ccx_messaging.publishers.sha_publisher.KafkaProducer"
        ) as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = SHAPublisher(outgoing_topic=topic_name, **producer_kwargs)

            # some error with type different from CCXMessagingError
            err = ArithmeticError("foobar")

            sut.error(input_msg, err)

    def _test_publish_wrong_message_encoding(self):
        """
        Test Producer.publish method when message can't be encoded to UTF-8.

        The kafka.KafkaProducer class is mocked in order to avoid the usage
        of the real library
        """
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        topic_name = "KAFKATOPIC"
        input_msg = ""
        message_to_publish = UnicodeEncodeErrorThrower()

        with patch(
            "ccx_messaging.publishers.sha_publisher.KafkaProducer"
        ) as kafka_producer_init_mock:
            producer_mock = MagicMock()
            kafka_producer_init_mock.return_value = producer_mock

            sut = SHAPublisher(outgoing_topic=topic_name, **producer_kwargs)

            with self.assertRaises(CCXMessagingError):
                sut.publish(input_msg, message_to_publish)
