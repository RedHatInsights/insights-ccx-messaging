"""Kafka consumer implementation for multiplexor messages using Confluent Kafka library."""

import json
import logging

import jsonschema
from confluent_kafka import Message

from ccx_messaging.consumers.kafka_consumer import KafkaConsumer
from ccx_messaging.error import CCXMessagingError
from ccx_messaging.schemas import IDENTITY_SCHEMA

LOG = logging.getLogger(__name__)


class DecodedIngressConsumer(KafkaConsumer):
    """Kafka consumer for decoded ingress messages produced by KafkaConsumer."""

    INPUT_MESSAGE_SCHEMA = {
        "type": "object",
        "properties": {
            "url": {"type": "string"},
            "identity": IDENTITY_SCHEMA,
            "timestamp": {"type": "string"},
            "cluster_name": {"type": ["string", "null"]},
        },
        "required": ["url", "identity", "timestamp"],
    }

    @staticmethod
    def parse_decoded_ingress_message(message: bytes) -> dict:
        """Parse a bytes messages into a dictionary, decoding encoded values."""
        try:
            deserialized_message = json.loads(message)
            jsonschema.validate(
                instance=deserialized_message, schema=DecodedIngressConsumer.INPUT_MESSAGE_SCHEMA
            )

        except TypeError as ex:
            LOG.warning("Incorrect message type: %s", message)
            raise CCXMessagingError("Incorrect message type") from ex

        except json.JSONDecodeError as ex:
            LOG.warning("Unable to decode received message: %s", message)
            raise CCXMessagingError("Unable to decode received message") from ex

        except jsonschema.ValidationError as ex:
            LOG.warning("Invalid input message JSON schema: %s", deserialized_message)
            raise CCXMessagingError("Invalid input message JSON schema") from ex

        LOG.debug("JSON schema validated: %s", deserialized_message)

        return deserialized_message

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

        deserialized_msg = self.parse_decoded_ingress_message(value)
        LOG.debug("JSON message deserialized (%s): %s", self.log_pattern, deserialized_msg)

        if not deserialized_msg.get("cluster_name"):
            cluster_id = (
                deserialized_msg.get("identity", {})
                .get("identity", {})
                .get("system", {})
                .get("cluster_id", None)
            )
            deserialized_msg["cluster_name"] = cluster_id

        return deserialized_msg
