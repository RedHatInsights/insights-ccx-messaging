# Copyright 2022 Red Hat Inc.
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

"""Module that implements a custom Kafka publisher."""

import logging

from insights_messaging.publishers import Publisher
from kafka import KafkaProducer

from ccx_messaging.error import CCXMessagingError

LOG = logging.getLogger(__name__)


class SHAPublisher(Publisher):
    """
    SHAPublisher based on the base Kafka publisher.

    The SHA records are received as a JSON (string)
    and turned into a byte array using UTF-8 encoding.
    The bytes are then sent to the output Kafka topic.

    Custom error handling for the whole pipeline is implemented here.
    """

    def __init__(self, outgoing_topic, bootstrap_servers, **kwargs):
        """Construct a new `SHAPublisher` given `kwargs` from the config YAML."""
        self.topic = outgoing_topic
        self.bootstrap_servers = bootstrap_servers
        if self.topic is None:
            raise KeyError("outgoing_topic")

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers, **kwargs
        )
        LOG.info(
            "Producing to topic '%s' on brokers %s", self.topic, self.bootstrap_servers
        )
        self.outdata_schema_version = 2

    def publish(self, message, response):
        """Publish the SHA records in the received JSON."""
        if response is not None:
            try:
                LOG.debug("Sending response to the %s topic.", self.topic)
                # Convert message string into a byte array.
                self.producer.send(self.topic, response.encode("utf-8"))
                LOG.debug("Message has been sent successfully.")
            except UnicodeEncodeError as err:
                raise CCXMessagingError(
                    f"Error encoding the response to publish: {message}"
                ) from err

    def error(self, input_msg, ex):
        """Handle pipeline errors by logging them."""
        # The super call is probably unnecessary because the default behavior
        # is to do nothing, but let's call it in case it ever does anything.
        super().error(input_msg, ex)

        if not isinstance(ex, CCXMessagingError):
            ex = CCXMessagingError(ex)

        LOG.error(ex.format(input_msg))
