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

"""Module implementing a ICM publisher to several Kafka topics depending on a filter."""

import gzip
import json
import logging
from typing import Any

from confluent_kafka import KafkaException, Producer
from insights_messaging.publishers import Publisher


log = logging.getLogger(__name__)
BEST_COMPRESSION = 9


class MultiplexorPublisher(Publisher):
    """A Kafka based publisher that sends the input message to the configured topics."""

    def __init__(
        self,
        outgoing_topics: dict[str, str] | None = None,
        kafka_broker_config: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        """Initialize a MultiplexorPublisher with the mapping between archive marks and topics.

        `outgoing_topics` is a dictionary that maps an archive "mark" (see `MultiplexorEngine`)
        to a Kafka topic. One archive can have several marks, so the input message can be sent
        to several Kafka topics.
        """
        self.topics_mapping = outgoing_topics if outgoing_topics is not None else {}

        if "compression" in kwargs:
            self.compression = kwargs.pop("compression")
        else:
            self.compression = None

        if kafka_broker_config:
            kwargs.update(kafka_broker_config)

        if "bootstrap.servers" not in kwargs:
            raise KafkaException("Broker not configured")

        log.debug(
            "Confluent Kafka consumer configuration arguments: Server: %s. Security protocol: %s.",
            kwargs.get("bootstrap.servers"),
            kwargs.get("security.protocol"),
        )
        self.producer = Producer(kwargs)
        log.info("Producing on brokers %s", kwargs.get("bootstrap.servers"))
        log.info("Topics mapping: %s", self.topics_mapping)

    def produce(self, outgoing_message: bytes, topic: str) -> None:
        """Send the message though the Kafka producer."""
        if self.compression:
            self.producer.produce(
                topic, gzip.compress(outgoing_message, compresslevel=BEST_COMPRESSION)
            )
        else:
            self.producer.produce(topic, outgoing_message)
        self.producer.poll(0)

    def publish(self, input_msg: dict[str, Any], response: set[str]) -> None:
        """Check to which topic should the `input_msg` be sent."""
        for mark in response:
            topic = self.topics_mapping.get(mark)
            if not topic:
                continue

            self.produce(json.dumps(input_msg).strip().encode(), topic)
