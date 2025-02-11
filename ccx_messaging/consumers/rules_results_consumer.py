"""Module containing the consumer for the Kafka topic produced by the Rules Processing service."""

import logging
import time
import os
from tempfile import NamedTemporaryFile
from typing import Any


from confluent_kafka import KafkaException
from insights import dr
from insights.core.exceptions import InvalidContentType
from insights_messaging.consumers import Consumer

from ccx_messaging.consumers.kafka_consumer import KafkaConsumer
from ccx_messaging.error import CCXMessagingError
from ccx_messaging.internal_pipeline import parse_rules_results_msg


LOG = logging.getLogger(__name__)

class RulesResultsConsumer(KafkaConsumer):
    """Consumer for the topic produced by `idp_rule_processing_publisher.IDPRuleProcessingPublisher`."""

    def get_url(self, input_msg: dict[str:str]) -> str:
        """Retrieve path to the archive in the S3 storage from Kafka message."""
        with NamedTemporaryFile() as file_data:
            file_data.write(input_msg)
            file_data.flush()
            return file_data.name
    
    def process_msg(self, msg):
        if not msg:
            LOG.debug("Empty record. Should not happen")
            return
        
        self.last_received_message_time = time.time()  # Base class thread control
        if msg.error():
            raise KafkaException(msg.error())
        
        try:
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
    
    def deserialize(self, msg):
        if not msg:
            raise CCXMessagingError("No incoming message: %s", msg)

        try:
            value = msg.value()
        except AttributeError as ex:
            raise CCXMessagingError("Invalid incoming message type: %s", type(msg)) from ex

        LOG.debug("Deserializing incoming message(%s): %s", self.log_pattern, value)

        if not value:
            raise CCXMessagingError("Unable to read incoming message: %s", value)

        deseralized_msg = parse_rules_results_msg(value)
        LOG.debug("JSON message deserialized (%s): %s", self.log_pattern, deseralized_msg)
        return deseralized_msg
    
    def create_broker(self, input_msg: dict[str, Any]) -> dr.Broker:
        """Create a suitable `Broker`."""
        broker = dr.Broker()
        broker["report_path"] = self.create_report_path(input_msg)
        return broker
    
    def create_report_path(self, input_msg):
        """
        Constructs report path from the original archive path
        """
        path, file_name = os.path.split(input_msg.get("path"))
        archive_name = file_name.replace(".tar.gz", "")
        new_path = path.replace("archives/compressed", "insights")
        report_path = os.path.join(new_path, archive_name, "insights.json")
        return report_path