# Copyright 2023 Red Hat Inc.
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

"""Module that implements a customizable Kafka publisher."""

import logging
import gzip

from confluent_kafka import KafkaException, Producer
from insights_messaging.publishers import Publisher

from ccx_messaging.error import CCXMessagingError

log = logging.getLogger(__name__)
BEST_COMPRESSION = 9


class KafkaPublisher(Publisher):
    """KafkaPublisher is a base class for Kafka based publishers.

    It relays on Confluent Kafka library to perform the Kafka related
    tasks.

    This is an "abstract" class, so it needs to be extended with the
    proper methods.
    """

    def __init__(self, outgoing_topic: str, kafka_broker_config: dict = None, **kwargs):
        """Construct a new `KafkaPubisher` given `kwargs` from the config YAML."""
        if not isinstance(outgoing_topic, str):
            raise CCXMessagingError("outgoing_topic should be a str")

        self.topic = outgoing_topic
        if "compression" in kwargs:
            self.compression = kwargs.pop("compression")
        else:
            self.compression = None

        if kafka_broker_config:
            kwargs.update(kafka_broker_config)

        if "bootstrap.servers" not in kwargs:
            raise KafkaException("Broker not configured")

        log.debug(
            "Confluent Kafka consumer configuration arguments: "
            "Server: %s. "
            "Topic: %s. "
            "Security protocol: %s.",
            kwargs.get("bootstrap.servers"),
            self.topic,
            kwargs.get("security.protocol"),
        )
        self.producer = Producer(kwargs)
        log.info(
            "Producing to topic '%s' on brokers %s", self.topic, kwargs.get("bootstrap.servers")
        )

    def produce(self, outgoing_message: bytes):
        """Send the message though the Kafka producer."""
        if self.compression:
            self.producer.produce(
                self.topic, gzip.compress(outgoing_message, compresslevel=BEST_COMPRESSION)
            )
        else:
            self.producer.produce(self.topic, outgoing_message)
        self.producer.poll(0)

    def publish(self, input_msg: dict, report: str):
        """Publish the report and other important info to Kafka.

        Default publish method, needs to be implemented in children classes.
        """
        raise NotImplementedError()

    def error(self, input_msg: dict, ex: Exception):
        """Handle pipeline errors by logging them."""
        log.warning("An error has ocurred during the processing of %s: %s", input_msg, ex)
