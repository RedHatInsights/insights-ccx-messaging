"""Kafka consumer implementation using Confluent Kafka library for Internal Data Pipeline."""
import logging
from confluent_kafka import Message
from ccx_messaging.consumers.kafka_consumer import KafkaConsumer
import json
from insights import dr

from ccx_messaging.error import CCXMessagingError

LOG = logging.getLogger(__name__)


class IDPConsumer(KafkaConsumer):

    """Consumer based in Confluent Kafka for Internal Data Pipeline."""

    def __init__(
        self,
        publisher,
        downloader,
        engine,
        **kwargs,
    ):
        """Initialise the KafkaConsumer object and related handlers."""
        kwargs.pop("requeuer", None)
        incoming_topic = kwargs.pop("incoming_topic")
        uploader = kwargs.pop("uploader", None)
        super().__init__(publisher, downloader, engine, incoming_topic, kwargs)

    def get_url(self, input_msg: dict) -> str:
        """Retrieve URL to storage from Kafka message."""
        return input_msg.get("path")

    def handles(self, msg: Message) -> bool:
        """Check if the message is usable."""
        message = self.deserialize(msg)
        return "path" in message

    def deserialize(self, msg):
        """Deserialize JSON message received from kafka."""
        try:
            deserialized_message = json.loads(msg.value())
        except TypeError as ex:
            LOG.warning("Incorrect message type: %s", msg)
            raise CCXMessagingError("Incorrect message type") from ex

        except json.JSONDecodeError as ex:
            LOG.warning("Unable to decode received message: %s", msg)
            raise CCXMessagingError("Unable to decode received message") from ex

        LOG.debug("JSON schema validated: %s", deserialized_message)
        return deserialized_message

    def create_broker(self, input_msg):
        """Create broker obj for IDP Consumer."""
        broker = dr.Broker()
        broker["cluster_id"] = input_msg.get("cluster_id")
        broker["s3_path"] = input_msg.get("path")
        return broker
