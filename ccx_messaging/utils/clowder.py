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
from tempfile import NamedTemporaryFile

import yaml
from app_common_python import LoadedConfig
from app_common_python.types import BrokerConfigAuthtypeEnum


def apply_clowder_config(manifest):
    """Apply Clowder config values to ICM config manifest."""
    Loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    config = yaml.load(manifest, Loader=Loader)

    clowder_broker_config = LoadedConfig.kafka.brokers[0]
    kafka_url = f"{clowder_broker_config.hostname}:{clowder_broker_config.port}"

    kafka_broker_config = {
        "bootstrap_servers": kafka_url,
    }

    if clowder_broker_config.cacert:
        # Current Kafka library is not able to handle the CA file, only a path to it
        with NamedTemporaryFile("w", delete=False) as temp_file:
            temp_file.write(clowder_broker_config.cacert)
            kafka_broker_config["ssl_cafile"] = temp_file.name

    if BrokerConfigAuthtypeEnum.valueAsString(clowder_broker_config.authtype) == "sasl":
        kafka_broker_config.update(
            {
                "sasl_mechanism": clowder_broker_config.sasl.saslMechanism,
                "sasl_plain_username": clowder_broker_config.sasl.username,
                "sasl_plain_password": clowder_broker_config.sasl.password,
                "security_protocol": clowder_broker_config.sasl.securityProtocol,
            }
        )

    config["service"]["consumer"]["kwargs"].update(kafka_broker_config)
    config["service"]["publisher"]["kwargs"].update(kafka_broker_config)

    pt_watcher = "ccx_messaging.watchers.payload_tracker_watcher.PayloadTrackerWatcher"
    for watcher in config["service"]["watchers"]:
        if watcher["name"] == pt_watcher:
            watcher["kwargs"].update(kafka_broker_config)
    return config
