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

    def test_init_no_topic(self):
        """Test SHAPublisher initializer without outgoing topic."""
        producer_kwargs = {
            "bootstrap_servers": ["kafka_server1"],
            "client_id": "ccx-data-pipeline",
        }

        with self.assertRaises(TypeError):
            _ = SHAPublisher(**producer_kwargs)
