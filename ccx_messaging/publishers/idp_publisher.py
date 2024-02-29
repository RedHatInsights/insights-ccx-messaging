"""Module implementing a IDP publisher to Kafka topic."""
import json
from typing import Dict
import logging
from ccx_messaging.error import CCXMessagingError
from ccx_messaging.publishers.kafka_publisher import KafkaPublisher
LOG = logging.getLogger(__name__)

class IDPPublisher(KafkaPublisher):

    """Publisher for interanal data pipeline."""

    def publish(self, input_msg: Dict, report: str) -> None:
        """Publish response as Kafka message to outgoing topic."""
        output_msg = {}
        try:
            output_msg = json.loads(report)
        except (TypeError, json.decoder.JSONDecodeError) as err:
            raise CCXMessagingError("Could not parse report; report is not in JSON format") from err
        output_msg.pop("reports", None)
        message = json.dumps(output_msg) + "\n"

        LOG.debug("Sending response to the %s topic.", self.topic)
        # Convert message string into a byte array.
        self.produce(message.encode("utf-8"))
        LOG.debug("Message has been sent successfully.")
        LOG.debug(
            "Message context: path=%s, original_path=%s, " 'metadata="%s"',
            output_msg["path"],
            output_msg["original_path"],
            output_msg["metadata"],
        )

        LOG.debug(
            "Status: Success; " "Topic: %s; " "Partition: %s; " "Offset: %s; ",
            input_msg.get("topic"),
            input_msg.get("partition"),
            input_msg.get("offset"),
        )
