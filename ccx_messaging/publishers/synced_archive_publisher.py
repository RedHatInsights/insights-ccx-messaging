# Copyright 2024 Red Hat, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may naot use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module implementing a IDP publisher to Kafka topic."""

import json
from typing import Dict
import logging
from ccx_messaging.publishers.kafka_publisher import KafkaPublisher

LOG = logging.getLogger(__name__)


class SyncedArchivePublisher(KafkaPublisher):
    """Publisher for interanal data pipeline."""  # noqa: D203

    def publish(self, input_msg: Dict, report: str) -> None:
        """Publish response as Kafka message to outgoing topic."""
        output_msg = json.loads(report)
        output_msg.pop("reports", None)
        output_msg["request_id"] = input_msg.get("request_id")
        message = json.dumps(output_msg)

        LOG.debug("Sending response to the %s topic.", self.topic)
        # Convert message string into a byte array.
        self.produce(message.encode("utf-8"))
        LOG.debug("Message has been sent successfully.")
        LOG.debug(
            'Message context: path=%s, original_path=%s, metadata="%s"',
            output_msg["path"],
            output_msg["original_path"],
            output_msg["metadata"],
        )
