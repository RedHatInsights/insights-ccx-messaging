"""Functions to adapt kafka-python configuration parameters."""

from kafka import KafkaProducer


def producer_config(config):
    """Clean up the provided configuration in order to be used by a Kafka producer."""
    producer_allowed_arguments = list(KafkaProducer.DEFAULT_CONFIG.keys())
    return {key: value for key, value in config.items() if key in producer_allowed_arguments}


def translate_kafka_configuration(config: dict) -> dict:
    """Transform a dict with default Kafka configuration to kafka-python configuration."""
    lib_config = {}

    if not config:
        return {}

    keys_translation = {
        "bootstrap.servers": "bootstrap_servers",
        "ssl.ca.location": "ssl_cafile",
        "sasl.mechanisms": "sasl_mechanism",
        "sasl.username": "sasl_plain_username",
        "sasl.password": "sasl_plain_password",
        "security.protocol": "security_protocol",
    }

    for kafka_key, lib_key in keys_translation.items():
        if kafka_key not in config:
            continue

        lib_config[lib_key] = config[kafka_key]

    return lib_config
