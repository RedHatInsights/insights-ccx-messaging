# Copyright 2022 Red Hat Inc.
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

"""Clowder integration functions."""

import logging

import yaml
import app_common_python
from app_common_python.types import BrokerConfigAuthtypeEnum


logger = logging.getLogger(__name__)


def apply_clowder_config(manifest):
    """Apply Clowder config values to ICM config manifest."""
    Loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    config = yaml.load(manifest, Loader=Loader)

    _add_buckets_config(config)
    _add_kafka_config(config)

    return config


# flake8: noqa: C901
def _add_kafka_config(config):
    # Find the Payload Tracker watcher, as it might be affected by config changes
    pt_watcher_name = "ccx_messaging.watchers.payload_tracker_watcher.PayloadTrackerWatcher"
    pt_watcher = None
    for watcher in config["service"]["watchers"]:
        if watcher["name"] == pt_watcher_name:
            pt_watcher = watcher
            break

    clowder_broker_config = app_common_python.LoadedConfig.kafka.brokers[0]
    kafka_urls = app_common_python.KafkaServers
    print("Kafka URLs:", kafka_urls)

    kafka_broker_config = {"bootstrap.servers": ",".join(kafka_urls)}

    if clowder_broker_config.cacert:
        # Current Kafka library is not able to handle the CA file, only a path to it
        # FIXME: Duplicating parameters in order to be used by both Kafka libraries
        ssl_ca_location = app_common_python.LoadedConfig.kafka_ca()
        kafka_broker_config["ssl.ca.location"] = ssl_ca_location

    if BrokerConfigAuthtypeEnum.valueAsString(clowder_broker_config.authtype) == "sasl":
        kafka_broker_config.update(
            {
                "sasl.mechanisms": clowder_broker_config.sasl.saslMechanism,
                "sasl.username": clowder_broker_config.sasl.username,
                "sasl.password": clowder_broker_config.sasl.password,
                "security.protocol": clowder_broker_config.sasl.securityProtocol,
            }
        )

    configure_consumer, configure_publisher = False, False
    if "kwargs" in config["service"]["consumer"]:
        print("Configuring consumer")
        configure_consumer = True
    if "kwargs" in config["service"]["publisher"]:
        print("Configuring publisher")
        configure_publisher = True

    if pt_watcher:
        pt_watcher["kwargs"]["kafka_broker_config"] = kafka_broker_config

    print("Kafka configuration updated from Clowder configuration")

    if configure_consumer:
        config["service"]["consumer"]["kwargs"]["kafka_broker_config"] = kafka_broker_config
        consumer_topic = config["service"]["consumer"]["kwargs"].get("incoming_topic")
        dlq_topic = config["service"]["consumer"]["kwargs"].get("dead_letter_queue_topic")
        if consumer_topic in app_common_python.KafkaTopics:
            topic_cfg = app_common_python.KafkaTopics[consumer_topic]
            config["service"]["consumer"]["kwargs"]["incoming_topic"] = topic_cfg.name
        else:
            print("The consumer topic cannot be found in Clowder mapping. It can cause errors")

        if dlq_topic in app_common_python.KafkaTopics:
            topic_cfg = app_common_python.KafkaTopics[dlq_topic]
            config["service"]["consumer"]["kwargs"]["dead_letter_queue_topic"] = topic_cfg.name

    if configure_publisher:
        config["service"]["publisher"]["kwargs"]["kafka_broker_config"] = kafka_broker_config
        producer_topic = config["service"]["publisher"]["kwargs"].get("outgoing_topic")
        if producer_topic in app_common_python.KafkaTopics:
            topic_cfg = app_common_python.KafkaTopics[producer_topic]
            config["service"]["publisher"]["kwargs"]["outgoing_topic"] = topic_cfg.name
        else:
            print("The publisher topic cannot be found in Clowder mapping. It can cause errors")

    payload_tracker_topic = pt_watcher["kwargs"].pop("topic") if pt_watcher else None

    if pt_watcher and payload_tracker_topic in app_common_python.KafkaTopics:
        topic_cfg = app_common_python.KafkaTopics[payload_tracker_topic]
        pt_watcher["kwargs"]["topic"] = topic_cfg.name
    else:
        print(
            "The Payload Tracker watcher topic cannot be found in Clowder mapping. "
            "It can cause errors",
        )


def _update_bucket_config(bucket_name, configuration):
    buckets = app_common_python.ObjectBuckets
    common_config = app_common_python.LoadedConfig.objectStore
    if not common_config:
        print("The clowder configuration doesn't provide object buckets configuration")
        return

    prefix = "https://" if common_config.tls else "http://"

    if bucket_name in buckets:
        bucket_config = buckets[bucket_name]
        configuration["access_key"] = bucket_config.accessKey
        configuration["secret_key"] = bucket_config.secretKey
        configuration["endpoint"] = f"{prefix}{common_config.hostname}:{common_config.port}"
    else:
        print("The bucket %s wasn't found among the Clowder buckets", bucket_name)


def _add_buckets_config(config):
    # Handle engine config
    engine = config.get("service", {}).get("engine", {})
    if engine.get("name") == "ccx_messaging.engines.s3_upload_engine.S3UploadEngine":
        engine_config = engine.get("kwargs", {})
        _update_bucket_config(engine_config.get("dest_bucket"), engine_config)

    # Handle downloader config
    downloader = config.get("service", {}).get("downloader", {})
    if downloader.get("name") == "ccx_messaging.downloaders.s3_downloader.S3Downloader":
        s3downloader_config = downloader.get("kwargs", {})
        _update_bucket_config(s3downloader_config.get("bucket"), s3downloader_config)
