import logging
from sentry_sdk import capture_exception
from confluent_kafka import Message
from ccx_messaging.consumers.kafka_consumer import KafkaConsumer
import json
LOG = logging.getLogger(__name__)
class Consumer(KafkaConsumer):
    def __init__(
        self,
        publisher,
        downloader,
        engine,
        **kwargs,
    ):
        """Initialise the KafkaConsumer object and related handlers."""
        requeuer = kwargs.pop("requeuer", None)
        incoming_topic = kwargs.pop("incoming_topic")
        super().__init__(publisher, downloader, engine,incoming_topic,kwargs)

    def get_url(self, input_msg: dict) -> str:  
        try:
            return input_msg.get("path")
        except:
            return "input msg has no attribute get"
        
    def handles(self, msg: Message) -> bool:
        return True
    
    def deserialize(self, msg):
        """
        Deserialize JSON message received from kafka
        """
        try:
            return json.loads(msg.value())
        except (TypeError, json.JSONDecodeError) as ex:
            LOG.error("Unable to deserialize JSON", extra=dict(ex=ex))
            capture_exception(ex)
            return None