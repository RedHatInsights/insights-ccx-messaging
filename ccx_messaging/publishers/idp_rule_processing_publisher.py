# Copyright 2025 Red Hat Inc.
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

"""Module that implements a custom Kafka publisher."""

import json
import logging
from typing import Any

import jsonschema

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.publishers.kafka_publisher import KafkaPublisher
from ccx_messaging.schemas import ARCHIVE_SYNCED_SCHEMA


log = logging.getLogger(__name__)


class IDPRuleProcessingPublisher(KafkaPublisher):
    """RuleProcessingPublisher handles the results of the applied rules and publish them to Kafka.

    The results of the data analysis are received as a JSON (string)
    and turned into a byte array using UTF-8 encoding.
    The bytes are then sent to the output Kafka topic.

    Custom error handling for the whole pipeline is implemented here.
    """

    def publish(self, input_msg: dict[str, Any], report: str | bytes) -> None:
        """Publish an EOL-terminated JSON message to the output Kafka topic.

        The report is assumed to be a string representing a valid JSON object.
        A newline character will be appended to it, it will be converted into
        a byte array using UTF-8 encoding and the result of that will be sent
        to the producer to produce a message in the output Kafka topic.
        """
        try:
            jsonschema.validate(input_msg, ARCHIVE_SYNCED_SCHEMA)

        except jsonschema.ValidationError as ex:
            raise CCXMessagingError("Invalid JSON format in the input message.") from ex

        try:
            report = json.loads(report)

        except (TypeError, json.decoder.JSONDecodeError):
            raise CCXMessagingError("Could not parse report; report is not in JSON format")

        output_msg = {
            "path": input_msg["path"],
            "metadata": input_msg["metadata"],
            "report": report,
            "request_id": input_msg.get("request_id"),
        }

        message = json.dumps(output_msg)
        log.debug("Sending response to the %s topic.", self.topic)
        # Convert message string into a byte array.
        self.produce(message.encode("utf-8"))
        log.debug("Message has been sent successfully.")
