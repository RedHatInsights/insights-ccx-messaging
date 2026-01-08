"""Functions to adapt kafka-python configuration parameters."""


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


def kafka_producer_config_cleanup(config):
    """Clean up the configuration dictionary of consumer-only properties."""
    consumer_only_properties = [
        "group.id",
        "session.timeout.ms",
        "heartbeat.interval.ms",
        "max.poll.interval.ms",
    ]

    for property in consumer_only_properties:
        if property in config:
            del config[property]

    return config
