# Copyright 2023 Red Hat Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test for the ccx_messaging.utils.kafka_config module."""

from ccx_messaging.utils.kafka_config import (
    translate_kafka_configuration,
    kafka_producer_config_cleanup,
)


def test_translate_kafka_configuration():
    """Check the translation of a default Kafka config to kafka-python configuration."""
    kafka_config = {
        "bootstrap.servers": "amazing-broker:9092",
        "ssl.ca.location": "/tmp/awesome.crt",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "incredible_user",
        "sasl.password": "supersecret_password",
        "security.protocol": "sasl_ssl",
    }

    expected_lib_config = {
        "bootstrap_servers": "amazing-broker:9092",
        "ssl_cafile": "/tmp/awesome.crt",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": "incredible_user",
        "sasl_plain_password": "supersecret_password",
        "security_protocol": "sasl_ssl",
    }

    assert translate_kafka_configuration(kafka_config) == expected_lib_config


def test_translate_kafka_configuration_unexpected_keys_ignored():
    """Check the translation ignore unexpected broker configuration prameters."""
    kafka_config = {
        "retry_backoff_ms": 500,
        "should_ignore_key": "I shouldn't be there",
        "sasl.password": "supersecret_password",
        "security.protocol": "sasl_ssl",
    }

    expected_lib_config = {
        "sasl_plain_password": "supersecret_password",
        "security_protocol": "sasl_ssl",
    }

    assert translate_kafka_configuration(kafka_config) == expected_lib_config


def test_kafka_producer_config_cleanup():
    """Check the clean up function for producer' configurations."""
    kafka_consumer_config = {
        "group.id": "should delete",
        "bootstrap.servers": "kafka:9092",
        "session.timeout.ms": "1000",
        "heartbeat.interval.ms": "1000",
        "max.poll.interval.ms": "1000",
    }

    cfg = kafka_producer_config_cleanup(kafka_consumer_config)

    assert "group.id" not in cfg
    assert "session.timeout.ms" not in cfg
    assert "heartbeat.interval.ms" not in cfg
    assert "max.poll.interval.ms" not in cfg
