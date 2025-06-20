"""Kafka consumer implementation using Confluent Kafka library for Internal Data Pipeline."""

import logging
import json
import re

from confluent_kafka import Message

from ccx_messaging.consumers.kafka_consumer import KafkaConsumer
from ccx_messaging.error import CCXMessagingError
from ccx_messaging.monitored_broker import SentryMonitoredBroker


# Path example: <org_id>/<cluster_id>/<year><month><day><time>-<id>
# Following RE matches with S3 archives like the previous example and allow
# to extract named groups for the different meaningful components
S3_ARCHIVE_PATTERN = re.compile(
    r"(?P<org_id>[0-9]+)\/"  # extract named group for organization id
    r"(?P<cluster_id>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\/"  # extract named group for the cluster_id  # noqa: E501
    r"(?P<archive>"  # extract named group for the archive name, including the following 3 lines
    r"(?P<timestamp>"  # extract the timestamp named group, including the following line
    # Next line extract year, month, day and time named groups from the timestamp
    r"(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})(?P<time>[0-9]{6}))-"
    r"(?P<id>[a-z,A-Z,0-9]*))"  # Extract the id of the file as named group
)
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
        super().__init__(publisher, downloader, engine, incoming_topic, **kwargs)

    def get_url(self, input_msg: dict) -> str:
        """Retrieve URL to storage from Kafka message."""
        return input_msg.get("path")

    def handles(self, msg: Message) -> bool:
        """Check if the message is usable."""
        message = self.deserialize(msg)
        return "path" in message

    def deserialize(self, msg):
        """Deserialize JSON message received from kafka."""
        if not msg:
            raise CCXMessagingError("No incoming message %s", msg)

        try:
            value = msg.value()

        except AttributeError as ex:
            raise CCXMessagingError("Invalid incoming message type: %s", type(msg)) from ex

        LOG.debug("Deserializing incoming message(%s): %s", self.log_pattern, value)

        try:
            deserialized_message = json.loads(value)

        except TypeError as ex:
            LOG.warning("Incorrect message type: %s", msg)
            raise CCXMessagingError("Incorrect message type") from ex

        except json.JSONDecodeError as ex:
            LOG.warning("Unable to decode received message: %s", msg)
            raise CCXMessagingError("Unable to decode received message") from ex

        LOG.debug("JSON schema validated: %s", deserialized_message)
        return deserialized_message

    def create_broker(self, input_msg):
        """Create a suitable `Broker` to be pass arguments to the `Engine`."""
        path = input_msg.get("path")
        broker = SentryMonitoredBroker()
        broker["original_path"] = path

        if "cluster_id" in input_msg:
            broker["cluster_id"] = input_msg["cluster_id"]

        match_ = S3_ARCHIVE_PATTERN.match(path)
        if not match_:
            LOG.warning("The archive doesn't match the expected pattern: %s", path)
            exception = CCXMessagingError("Archive pattern name incorrect")
            self.fire("on_consumer_failure", broker, exception)
            raise exception

        # Cluster ID might be overrided by the one found in the `path`
        for key, value in match_.groupdict().items():
            if key not in broker:
                broker[key] = value

        return broker
