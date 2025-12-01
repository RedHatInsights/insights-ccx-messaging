"""Kafka consumer implementation using Confluent Kafka library."""

import importlib.metadata
import logging
import time
from datetime import datetime
from threading import Thread

from confluent_kafka import (
    Consumer as ConfluentConsumer,
    KafkaException,
    Message,
    Producer,
    TIMESTAMP_NOT_AVAILABLE,
)
from insights.core.exceptions import InvalidContentType
from insights_messaging.consumers import Consumer

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.ingress import parse_ingress_message
from ccx_messaging.monitored_broker import SentryMonitoredBroker
from ccx_messaging.utils.kafka_config import kafka_producer_config_cleanup


LOG = logging.getLogger(__name__)
MAX_ELAPSED_TIME_BETWEEN_MESSAGES = 60 * 60


class KafkaConsumer(Consumer):
    """Consumer based in Confluent Kafka."""

    def __init__(
        self,
        publisher,
        downloader,
        engine,
        incoming_topic,
        kafka_broker_config=None,
        platform_service=None,
        dead_letter_queue_topic=None,
        max_record_age=7200,
        processing_timeout_s=0,
        **kwargs,
    ):
        """Initialise the KafkaConsumer object and related handlers."""
        requeuer = kwargs.pop("requeuer", None)
        super().__init__(publisher, downloader, engine, requeuer=requeuer)

        if kafka_broker_config:
            kwargs.update(kafka_broker_config)

        # Confluent initialization
        LOG.info(
            "Consuming topic '%s' from brokers %s as group '%s'",
            incoming_topic,
            kwargs.get("bootstrap.servers", None),
            kwargs.get("group.id", None),
        )

        LOG.debug("Confluent Kafka consumer extra configuration arguments: %s", kwargs)

        self.consumer = ConfluentConsumer(kwargs)
        self.consumer.subscribe([incoming_topic])

        # Self handled vars
        self.log_pattern = f"topic: {incoming_topic}, group.id: {kwargs.get('group.id', None)}"

        # Service to filter in messages
        self.platform_service = platform_service

        self.max_record_age = max_record_age
        self.last_received_message_time = time.time()
        self.check_elapsed_time_thread = Thread(
            target=self.check_last_message_received_time, daemon=True
        )
        self.check_elapsed_time_thread.start()

        self.processing_timeout = processing_timeout_s

        # DLQ
        self.dlq_producer = None
        self.dead_letter_queue_topic = dead_letter_queue_topic

        if self.dead_letter_queue_topic is not None:
            self.dlq_producer = Producer(kafka_producer_config_cleanup(kwargs))

        try:
            self.ocp_rules_version = importlib.metadata.version("ccx-rules-ocp")

        except importlib.metadata.PackageNotFoundError:
            logging.info("No OCP rules package is installed.")
            self.ocp_rules_version = None

    def get_url(self, input_msg: dict) -> str:
        """Retrieve URL to storage (S3/Minio) from Kafka message.

        Same as previous 2 methods, when we receive and figure out the
        message format, we can modify this method
        """
        try:
            url = input_msg["url"]
            LOG.debug(
                "Extracted URL from input message: %s",
                url,
            )
            return url

        # This should never happen, but let's check it just to be absolutely sure.
        # The `handles` method should prevent this from
        # being called if the input message format is wrong.
        except Exception as ex:
            LOG.warning("Unable to extract URL from input message: %s", ex)
            raise CCXMessagingError("Unable to extract URL from input message") from ex

    def run(self):
        """Consume message and proccess."""
        try:
            while True:
                received = self.consumer.consume(timeout=5.0)  # return empty if timeout

                for msg in received:
                    self.process_msg(msg)

        except KeyboardInterrupt:
            LOG.info("Cancelled by user")

        except KafkaException as ex:
            LOG.fatal("Fatal error: %s", ex)

        finally:
            LOG.info("Closing consumer")
            self.consumer.close()

    def handles(self, msg: Message) -> bool:
        """Check headers, format and other characteristics that can make the message unusable."""
        if self.platform_service:
            headers = msg.headers()
            if not headers:
                LOG.debug("Message filtered: no headers in message")
                return False

            headers = dict(headers)
            destination_service = headers.get("service", b"").decode()

            if destination_service != self.platform_service:
                LOG.debug("Message filtered: wrong destination service: %s", destination_service)
                self.fire("on_filter")
                return False

        return self._handles_timestamp_check(msg)

    def _handles_timestamp_check(self, msg: Message):
        """Check the timestamp of the msg."""
        if self.max_record_age == -1:
            return True

        timestamp_type, timestamp = msg.timestamp()

        if timestamp_type == TIMESTAMP_NOT_AVAILABLE:
            LOG.debug("Cannot check the incoming message timestamp.")
            return True

        # Kafka record timestamp is int64 in milliseconds.
        current = time.time()
        if (timestamp / 1000) < (current - self.max_record_age):
            LOG.debug("Skipping message due to its timestamp (too old)")
            return False

        return True

    def process_msg(self, msg: Message) -> None:
        """Process the message for the engine."""
        if not msg:
            LOG.debug("Empty record. Should not happen")
            return

        self.last_received_message_time = time.time()
        if msg.error():
            raise KafkaException(msg.error())

        if not self.handles(msg):
            # already logged in self.handles
            return

        try:
            if self.ocp_rules_version:
                LOG.debug("Processing message using OCP rules version %s", self.ocp_rules_version)
            # Deserialize
            value = self.deserialize(msg)

            # Core Messaging process
            self.process(value)

        except InvalidContentType as ex:
            LOG.warning("The archive cannot be processed by Insights: %s", ex)
            self.process_dead_letter(msg)

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

        deserialized_msg = parse_ingress_message(value)

        if not deserialized_msg.get("cluster_name"):
            cluster_id = (
                deserialized_msg.get("identity", {})
                .get("identity", {})
                .get("system", {})
                .get("cluster_id", None)
            )
            deserialized_msg["cluster_name"] = cluster_id

        return deserialized_msg

    def check_last_message_received_time(self):
        """Verify elapsed time between received messages and warn if too long.

        Checks if the last received message was received more than one hour ago
        and sends an alert if it is the case
        """
        while True:
            if time.time() - self.last_received_message_time >= MAX_ELAPSED_TIME_BETWEEN_MESSAGES:
                LOG.warning("No new messages in the queue")
            # To do the minimum interruptions possible, sleep for one hour
            time.sleep(MAX_ELAPSED_TIME_BETWEEN_MESSAGES)

    def create_broker(self, input_msg):
        """Create a suitable `Broker` to be pass arguments to the `Engine`."""
        broker = SentryMonitoredBroker()

        # Some engines expect some data for its own usage, like the following:
        # (NOTE: The fields should be always present because of schema validation
        # done in the other method, default values are just a sanity check.)
        org_id = (
            input_msg.get("identity", {})
            .get("identity", {})
            .get("internal", {})
            .get("org_id", None)
        )
        date = datetime.fromisoformat(input_msg.get("timestamp", "0"))

        broker["org_id"] = org_id
        broker["cluster_id"] = input_msg["cluster_name"]
        broker["original_path"] = input_msg["url"]
        broker["year"] = f"{date.year:04}"
        broker["month"] = f"{date.month:02}"
        broker["day"] = f"{date.day:02}"
        broker["hour"] = f"{date.hour:02}"
        broker["minute"] = f"{date.minute:02}"
        broker["second"] = f"{date.second:02}"
        broker["time"] = f"{date.hour:02}{date.minute:02}{date.second:02}"

        return broker

    def process_dead_letter(self, msg: Message) -> None:
        """Send the message to a dead letter queue in a different Kafka topic."""
        if not self.dlq_producer:
            return

        LOG.info("Sending the message to dead letter queue topic")
        self.dlq_producer.produce(
            self.dead_letter_queue_topic,
            msg.value(),
        )
