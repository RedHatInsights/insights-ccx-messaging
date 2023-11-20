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

"""Module implementing a DVO Metrics publisher to Kafka topic."""

import json
import logging
from typing import Dict

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.publishers.kafka_publisher import KafkaPublisher


log = logging.getLogger(__name__)


class DVOMetricsPublisher(KafkaPublisher):

    """DVOMetricsPublisher handles the result of the extraction of DVO metrics from an archive."""

    def publish(self, input_msg: Dict, report: str) -> None:
        """Publish an EOL-terminated JSON message to the output Kafka topic.

        The response is assumed to be a string representing a valid JSON object.
        A newline character will be appended to it, it will be converted into
        a byte array using UTF-8 encoding and the result of that will be sent
        to the producer to produce a message in the output Kafka topic.
        """
        output_msg = {}
        try:
            org_id = int(input_msg["identity"]["identity"]["internal"]["org_id"])
        except (ValueError, KeyError, TypeError) as err:
            raise CCXMessagingError(f"Error extracting the OrgID: {err}") from err

        try:
            account_number = int(input_msg["identity"]["identity"]["account_number"])
        except (ValueError, KeyError, TypeError) as err:
            log.warning(f"Error extracting the Account number: {err}")
            account_number = ""

        if "cluster_name" not in input_msg:
            raise CCXMessagingError()

        output_msg = {
            "OrgID": org_id,
            "AccountNumber": account_number,
            "ClusterName": input_msg["cluster_name"],
            "Metrics": json.loads(report),
            "RequestId": input_msg.get("request_id"),
        }
        message = json.dumps(output_msg) + "\n"

        log.debug("Sending response to the %s topic.", self.topic)
        # Convert message string into a byte array.
        self.produce(message.encode("utf-8"))
        log.debug("Message has been sent successfully.")
        log.debug(
            "Message context: OrgId=%s, AccountNumber=%s, "
            'ClusterName="%s"',
            output_msg["OrgID"],
            output_msg["AccountNumber"],
            output_msg["ClusterName"],
        )

        log.debug(
            "Status: Success; "
            "Topic: %s; "
            "Partition: %s; "
            "Offset: %s; ",
            input_msg.get("topic"),
            input_msg.get("partition"),
            input_msg.get("offset"),
        )
