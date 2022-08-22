"""Functions to adapt kafka-python configuration parameters."""

from kafka import KafkaProducer


def producer_config(config):
    """Clean up the provided configuration in order to be used by a Kafka producer."""
    producer_allowed_arguments = list(KafkaProducer.DEFAULT_CONFIG.keys())
    return {key: value for key, value in config.items() if key in producer_allowed_arguments}
