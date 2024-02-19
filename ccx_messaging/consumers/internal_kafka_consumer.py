"""Kafka consumer implementation using Confluent Kafka library for Internal Data Pipeline."""
import logging
from sentry_sdk import capture_exception
from confluent_kafka import Message
from ccx_messaging.consumers.kafka_consumer import KafkaConsumer
import json
LOG = logging.getLogger(__name__)
class Consumer(KafkaConsumer):

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
        super().__init__(publisher, downloader, engine,incoming_topic,kwargs)

    def get_url(self, input_msg: dict) -> str:
        """Retrieve URL to storage from Kafka message."""
        try:
            return input_msg.get("path")
        except Exception:
            return "input msg has no attribute get"

    def handles(self, msg: Message) -> bool:
        """Check if the message is usable."""
        message = self.deserialize(msg.value())
        if message.get("path") is not None:
            return True
        else:
            return False

    def deserialize(self, msg):
        """Deserialize JSON message received from kafka."""
        try:
            return json.loads(msg.value())
        except (TypeError, json.JSONDecodeError) as ex:
            LOG.error("Unable to deserialize JSON", extra=dict(ex=ex))  # noqa: C408
            capture_exception(ex)
            return None
