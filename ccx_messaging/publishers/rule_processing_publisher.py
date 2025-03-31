# Copyright 2019, 2020, 2021, 2022 Red Hat Inc.
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

import datetime
import json
import logging
from json import JSONDecodeError

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.publishers.kafka_publisher import KafkaPublisher


log = logging.getLogger(__name__)

RFC3339_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class RuleProcessingPublisher(KafkaPublisher):
    """RuleProcessingPublisher handles the results of the applied rules and publish them to Kafka.

    The results of the data analysis are received as a JSON (string)
    and turned into a byte array using UTF-8 encoding.
    The bytes are then sent to the output Kafka topic.

    Custom error handling for the whole pipeline is implemented here.
    """

    def __init__(self, outgoing_topic, kafka_broker_config=None, **kwargs):
        """Construct a new `RuleProcessingPublisher` given `kwargs` from the config YAML."""
        super().__init__(outgoing_topic, kafka_broker_config, **kwargs)

    def validate_timestamp_rfc3339(self, timestamp):
        """Check if the timestamp matches RFC3339 format."""
        try:
            datetime.datetime.strptime(timestamp, RFC3339_FORMAT)
        except:  # noqa E722
            return False
        return True

    def get_gathering_time(self, input_msg):
        """Retrieve the gathering time from input message if present, otherwise create one."""
        gathered_at = (
            input_msg.get("metadata", {}).get("custom_metadata", {}).get("gathering_time", None)
        )

        if not gathered_at:
            log.debug("Gathering time is not present; creating replacement")
            gathered_at = datetime.datetime.now().strftime(RFC3339_FORMAT)

        # If the timestamp is not in correct format, try to parse
        # format used in molodec. Otherwise use current timestamp.
        if not self.validate_timestamp_rfc3339(gathered_at):
            try:
                gathered_at = datetime.datetime.fromisoformat(gathered_at).strftime(RFC3339_FORMAT)
                log.debug("Converting gathering time from ISO format to RFC3339 format")
            except ValueError:
                log.debug("Gathering time could not be parsed; creating replacement")
                gathered_at = datetime.datetime.now().strftime(RFC3339_FORMAT)

        return gathered_at

    def publish(self, input_msg, report):
        """Publish an EOL-terminated JSON message to the output Kafka topic.

        The report is assumed to be a string representing a valid JSON object.
        A newline character will be appended to it, it will be converted into
        a byte array using UTF-8 encoding and the result of that will be sent
        to the producer to produce a message in the output Kafka topic.
        """
        try:
            report = json.loads(report)
        except (TypeError, json.decoder.JSONDecodeError):
            raise CCXMessagingError("Could not parse report; report is not in JSON format")

        if "reports" not in report.keys():
            log.debug("Report does not contain OCP rules related results; skipping")
            return

        report.pop("workload_recommendations", None)

        output_msg = {}
        try:
            org_id = int(input_msg["identity"]["identity"]["internal"]["org_id"])
        except (ValueError, KeyError, TypeError) as err:
            log.warning("Error extracting the OrgID: %s", err)
            raise CCXMessagingError("Error extracting the OrgID") from err

        try:
            account_number = int(input_msg["identity"]["identity"]["account_number"])
        except (ValueError, KeyError, TypeError) as err:
            log.warning("Error extracting the Account number: %s", err)
            account_number = ""

        try:
            msg_timestamp = input_msg["timestamp"]
            msg_version = report.pop("version", 0)
            output_msg = {
                "OrgID": org_id,
                "AccountNumber": account_number,
                "ClusterName": input_msg["cluster_name"],
                "Report": report,
                "LastChecked": msg_timestamp,
                "Version": msg_version,
                "RequestId": input_msg.get("request_id"),
                "Metadata": {"gathering_time": self.get_gathering_time(input_msg)},
            }

            message = json.dumps(output_msg) + "\n"

            log.debug("Sending response to the %s topic.", self.topic)
            # Convert message string into a byte array.
            self.produce(message.encode("utf-8"))
            log.debug("Message has been sent successfully.")
            log.debug(
                "Message context: OrgId=%s, AccountNumber=%s, "
                'ClusterName="%s", LastChecked="%s, Version=%d"',
                output_msg["OrgID"],
                output_msg["AccountNumber"],
                output_msg["ClusterName"],
                output_msg["LastChecked"],
                output_msg["Version"],
            )

        except KeyError as err:
            raise CCXMessagingError("Missing expected keys in the input message") from err

        except (TypeError, UnicodeEncodeError, JSONDecodeError) as err:
            log.warning(err)
            raise CCXMessagingError("Error encoding the response to publish") from err
