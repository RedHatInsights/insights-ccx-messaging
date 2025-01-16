"""Module containing the consumer for the Kafka topic produced by the Archive Sync service."""

import logging
from typing import Any

from confluent_kafka import Message, KafkaException
from insights import dr
from insights_messaging.consumers import Consumer

from ccx_messaging.consumers.kafka_consumer import KafkaConsumer
from ccx_messaging.internal_pipeline import parse_archive_sync_msg
from ccx_messaging.error import CCXMessagingError


LOG = logging.getLogger(__name__)


class SyncedArchiveConsumer(KafkaConsumer):
    """Consumer for the topic produced by `synced_archive_publisher.SyncedArchivePublisher`."""

    def get_url(self, input_msg: dict[str:str]) -> str:
        """Retrieve path to the archive in the S3 storage from Kafka message."""
        # it's safe to asume the "path" is there because the message format is validated
        return input_msg["path"]

    def process_msg(self, msg: Message) -> None:
        """Process a single message received from the topic."""
        if not msg:
            LOG.debug("Empty record. Should not happen")
            return

        if msg.error():
            raise KafkaException(msg.error())

        try:
            # Deserialize
            value = self.deserialize(msg)
            # Core Messaging process
            self.process(value)

        except CCXMessagingError as ex:
            LOG.warning(
                "Unexpected error processing incoming message. (%s): %s. Error: %s",
                self.log_pattern,
                msg.value(),
                ex,
            )
            self.process_dead_letter(msg)

        except TimeoutError as ex:
            self.fire("on_process_timeout")
            LOG.exception(ex)
            self.process_dead_letter(msg)

        except Exception as ex:  # pylint: disable=broad-exception-caught
            LOG.exception(ex)
            self.process_dead_letter(msg)

    def deserialize(self, msg: Message) -> dict:
        """Deserialize the message received from Kafka into a dictionary."""
        if not msg:
            raise CCXMessagingError("No incoming message: %s", msg)

        try:
            value = msg.value()
        except AttributeError as ex:
            raise CCXMessagingError("Invalid incoming message type: %s", type(msg)) from ex

        LOG.debug("Deserializing incoming message(%s): %s", self.log_pattern, value)

        if not value:
            raise CCXMessagingError("Unable to read incoming message: %s", value)

        deseralized_msg = parse_archive_sync_msg(value)
        LOG.debug("JSON message deserialized (%s): %s", self.log_pattern, deseralized_msg)
        return deseralized_msg

    def create_broker(self, input_msg: dict[str, Any]) -> dr.Broker:
        """Create a suitable `Broker`."""
        return Consumer.create_broker(self, input_msg)