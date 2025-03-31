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

"""Module that implements a Confluent based Kafka publisher to send the workload info."""

import json
import logging
from json import JSONDecodeError

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.publishers.kafka_publisher import KafkaPublisher


log = logging.getLogger(__name__)


class WorkloadInfoPublisher(KafkaPublisher):
    """WorkloadInfoPublisher based on Confluent Kafka Producer.

    The workload info is received from a custom engine as a JSON string.
    The response, among other information extracted from the consumer's received
    message, is sent over a Kafka topic to be consumed by other services.
    """

    def __init__(self, outgoing_topic, kafka_broker_config=None, **kwargs):
        """Construct a new `RuleProcessingPublisher` given `kwargs` from the config YAML."""
        super().__init__(outgoing_topic, kafka_broker_config, **kwargs)
        self.outdata_schema_version = 2

    def publish(self, input_msg: dict, response: str | None):
        """Publish an EOL-terminated JSON message to the output Kafka topic.

        The input_msg contains content of message read from incoming Kafka
        topic. Such message should contains account info, cluster ID etc.

        The response is assumed to be a string representing a valid JSON object
        (it is read from file config/workload_info.json).

        Outgoing message is constructed by joining input_msg with response.

        A newline character will be appended to it, it will be converted into
        a byte array using UTF-8 encoding and the result of that will be sent
        to the producer to produce a message in the output Kafka topic.
        """
        try:
            org_id = int(input_msg["identity"]["identity"]["internal"]["org_id"])
        except (ValueError, TypeError, KeyError) as err:
            log.warning("Error extracting the OrgID: %s", err)
            raise CCXMessagingError("Error extracting the OrgID") from err

        try:
            account_number = int(input_msg["identity"]["identity"]["account_number"])
        except (ValueError, KeyError, TypeError) as err:
            log.warning("Error extracting the Account number: %s", err)
            account_number = ""

        # outgoing message in form of JSON
        message = ""

        if response is None:
            log.debug("No response was generated for this archive. Skipping")
            return

        try:
            msg_timestamp = input_msg["timestamp"]
            output_msg = {
                "OrgID": org_id,
                "AccountNumber": account_number,
                "ClusterName": input_msg["cluster_name"],
                "Images": json.loads(response),
                "LastChecked": msg_timestamp,
                "Version": self.outdata_schema_version,
                "RequestId": input_msg.get("request_id"),
            }

            # convert dictionary to JSON (string)
            message = json.dumps(output_msg) + "\n"

            log.debug("Sending response to the %s topic.", self.topic)

            # Convert message string into a byte array.
            self.produce(message.encode("utf-8"))
            log.debug("Message has been sent successfully.")
            log.debug(
                "Message context: OrgId=%s, AccountNumber=%s, "
                'ClusterName="%s", NumImages: %d, LastChecked="%s, Version=%d"',
                output_msg["OrgID"],
                output_msg["AccountNumber"],
                output_msg["ClusterName"],
                len(output_msg["Images"]),
                output_msg["LastChecked"],
                output_msg["Version"],
            )

        except (KeyError, TypeError, UnicodeEncodeError, JSONDecodeError) as err:
            log.warning("Error encoding the response to publish: %s", message)
            raise CCXMessagingError("Error encoding the response to publish") from err
