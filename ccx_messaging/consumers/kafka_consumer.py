"""Kafka consumer implementation using Confluent Kafka library."""

import logging
import time
from threading import Thread

from confluent_kafka import Consumer as ConfluentConsumer, KafkaException, Message
from insights_messaging.consumers import Consumer

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.ingress import parse_ingress_message


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
        platform_service=None,
        dead_letter_queue_topic=None,
        max_record_age=7200,
        retry_backoff_ms=1000,
        processing_timeout_s=0,
        **kwargs,
    ):
        """Initialise the KafkaConsumer object and related handlers."""
        requeuer = kwargs.pop("requeuer", None)
        super().__init__(publisher, downloader, engine, requeuer=requeuer)

        LOG.info(
            "Consuming topic '%s' from brokers %s as group '%s'",
            incoming_topic,
            kwargs.get("bootstrap_servers", None),
            kwargs.get("group_id", None),
        )

        # Confluent initialization
        
        config = kwargs.copy()
        config.update(
            {
                "bootstrap.servers": kwargs.get("bootstrap_servers", ""),
                "group.id": kwargs.get("group_id", None),
                "retry.backoff.ms": retry_backoff_ms,
            }
        )
        if "bootstrap_servers" in config:
            del config["bootstrap_servers"]
        if "group_id" in config:
            del config["group_id"]

        self.consumer = ConfluentConsumer(config)
        self.consumer.subscribe([incoming_topic])

        # Self handled vars
        self.log_pattern = f"topic: {incoming_topic}, group_id: {kwargs.get('group_id', None)}"

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
            # dlq_producer_config = producer_config(kwargs)
            # self.dlq_producer = KafkaProducer(**dlq_producer_config)
            LOG.warning(
                "DLQ is currently disabled while the libraries are migrated to Confluent Kafka."
            )

    def get_url(self, input_msg: dict) -> str:
        """
        Retrieve URL to storage (S3/Minio) from Kafka message.

        Same as previous 2 methods, when we receive and figure out the
        message format, we can modify this method
        """
        try:
            url = input_msg["url"]
            LOG.debug(
                "Extracted URL from input message: %s (%s)",
                url,
                get_stringfied_record(input_msg),
            )
            return url

        # This should never happen, but let's check it just to be absolutely sure.
        # The `handles` method should prevent this from
        # being called if the input message format is wrong.
        except Exception as ex:
            raise CCXMessagingError(f"Unable to extract URL from input message: {ex}") from ex

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
        if not self.platform_service:
            return True

        headers = msg.headers()
        if not headers:
            LOG.debug("Message filtered: no headers in message")
            return False

        headers = dict(headers)
        destination_service = headers.get("service", b"").decode()

        if destination_service != self.platform_service:
            LOG.debug("Message filtered: wrong detination service: %s", destination_service)
            return False

        return True

    def process_msg(self, msg: Message) -> None:
        """Process the message for the engine."""
        if not msg:
            LOG.debug("Empty record. Should not happen")
            return

        if msg.error():
            raise KafkaException(msg.error())

        if not self.handles(msg):
            # already logged in self.handles
            return

        try:
            # Deserialize
            value = self.deserialize(msg)

            # Enrich the deserialized message with some context info
            value["topic"] = msg.topic()
            value["partition"] = msg.partition()
            value["offset"] = msg.offset()

            # Core Messaging process
            self.process(value)

        except CCXMessagingError as ex:
            LOG.warning(
                "Unexpected error deserializing incoming message. (%s): %s. Error: %s",
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

        deseralized_msg = parse_ingress_message(value)
        LOG.debug("JSON message deserialized (%s): %s", self.log_pattern, deseralized_msg)

        cluster_id = deseralized_msg.get("identity", {}).get("system", {}).get("cluster_id", None)
        deseralized_msg["cluster_name"] = cluster_id
        return deseralized_msg

    def check_last_message_received_time(self):
        """
        Verify elapsed time between received messages and warn if too long.

        Checks if the last received message was received more than one hour ago
        and sends an alert if it is the case
        """
        while True:
            if time.time() - self.last_received_message_time >= MAX_ELAPSED_TIME_BETWEEN_MESSAGES:
                last_received_time_str = time.strftime(
                    "%Y-%m-%d- %H:%M:%S", time.gmtime(self.last_received_message_time)
                )
                LOG.warning("No new messages in the queue since %s", last_received_time_str)
            # To do the minimum interruptions possible, sleep for one hour
            time.sleep(MAX_ELAPSED_TIME_BETWEEN_MESSAGES)

    def process_dead_letter(self, msg: Message) -> None:
        """Send the message to a dead letter queue in a different Kafka topic."""
        if not self.dlq_producer:
            return

        if isinstance(msg, Message):
            self.dlq_producer.send(
                self.dead_letter_queue_topic,
                msg.value(),
            )

        else:
            # just add at least some record in case that the message is not of the expected type
            self.dlq_producer.send(
                self.dead_letter_queue_topic,
                str(msg).encode("utf-8"),
            )


def get_stringfied_record(input_record: dict) -> str:
    """Retrieve a string with information about the received record ready to log."""
    return (
        f"topic: '{input_record.get('topic')}', partition: {input_record.get('partition')}, "
        f"offset: {input_record.get('offset')}, timestamp: {input_record.get('timestamp')}"
    )
