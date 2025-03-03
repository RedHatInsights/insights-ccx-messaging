# Copyright 2024 Red Hat, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may naot use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the MultiplexorPublisher class."""

import json
import gzip
from unittest.mock import call, MagicMock

from ccx_messaging.publishers.multiplexor_kafka_publisher import MultiplexorPublisher


BEST_COMPRESSION = 9
INPUT_MSG = {
    "path": "target/path",
    "cluster_id": "05ef94c8-9468-405e-93cc-bef1e79cde96",
    "sqs_message_id": "AQEBt8CEyiFnyVTY+B2kuJ+gOpCcMsev",
}


def timeStampMasking(message):
    """Mask four bytes in Gzip stream that contain timestamp."""
    message = list(message)
    message[4] = 0
    message[5] = 0
    message[6] = 0
    message[7] = 0
    message = bytes(message)
    return message


def test_init():
    """Check that init creates a valid object."""
    kakfa_config = {
        "bootstrap.servers": "kafka:9092",
    }
    MultiplexorPublisher(
        outgoing_topics={
            "OLS": "ols-topic",
            "DEFAULT": "io-topic",
        },
        **kakfa_config,
    )


def test_normal_produce():
    """Check if message is not gzipped if compression is disabled."""
    topic = "io-topic"
    kakfa_config = {"bootstrap.servers": "kafka:9092"}
    pub = MultiplexorPublisher(**kakfa_config)

    pub.producer = MagicMock()
    pub.produce(json.dumps(INPUT_MSG).strip().encode(), topic)
    pub.producer.produce.assert_called_with(topic, json.dumps(INPUT_MSG).strip().encode())


def test_compressed_produce():
    """Check if message is not gzipped if compression is disabled."""
    topic = "io-topic"
    kakfa_config = {"bootstrap.servers": "kafka:9092"}
    pub = MultiplexorPublisher(compression="gzip", **kakfa_config)

    pub.producer = MagicMock()
    pub.produce(json.dumps(INPUT_MSG).strip().encode(), topic)

    produce_mock_call = pub.producer.produce.call_args[0]
    assert topic == produce_mock_call[0]

    sent_data = json.loads(gzip.decompress(produce_mock_call[1]))
    assert sent_data == INPUT_MSG


def test_with_known_marks():
    """Check if the message is sent to the expected topics."""
    input_msg_bytes = json.dumps(INPUT_MSG).strip().encode()

    topics_mapping = {
        "MARK1": "topic1",
        "MARK2": "topic2",
    }

    kakfa_config = {"bootstrap.servers": "kafka:9092"}
    pub = MultiplexorPublisher(outgoing_topics=topics_mapping, **kakfa_config)

    pub.producer = MagicMock()

    pub.publish(INPUT_MSG, {"MARK1", "MARK2"})
    pub.producer.produce.assert_has_calls(
        [
            call("topic1", input_msg_bytes),
            call("topic2", input_msg_bytes),
        ],
        any_order=True,
    )


def test_with_unknown_marks():
    """Check if the message is not sent to any topics."""
    topics_mapping = {
        "MARK1": "topic1",
        "MARK2": "topic2",
    }

    kakfa_config = {"bootstrap.servers": "kafka:9092"}
    pub = MultiplexorPublisher(outgoing_topics=topics_mapping, **kakfa_config)

    pub.producer = MagicMock()

    pub.publish(INPUT_MSG, {"MARK3"})
    pub.producer.produce.assert_not_called()

    pub.publish(INPUT_MSG, set())
    pub.producer.produce.assert_not_called()
