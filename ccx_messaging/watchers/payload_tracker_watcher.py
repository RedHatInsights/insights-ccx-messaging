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

"""Module containing a `Watcher` to send updates to the `payload-tracker` service."""

import datetime
import json
import logging

from confluent_kafka import Producer

from ccx_messaging.utils.kafka_config import kafka_producer_config_cleanup
from ccx_messaging.watchers.consumer_watcher import ConsumerWatcher


LOG = logging.getLogger(__name__)


class PayloadTrackerWatcher(ConsumerWatcher):
    """`Watcher` implementation to handle Payload Tracker updates."""

    def __init__(self, topic, service_name="ccx-data-pipeline", kafka_broker_config=None, **kwargs):
        """Construct a `PayloadTrackerWatcher` object."""
        self.topic = topic

        if not self.topic:
            raise KeyError("topic")

        if kafka_broker_config:
            kwargs.update(kafka_broker_config)

        kwargs = kafka_producer_config_cleanup(kwargs)

        LOG.info(
            "Confluent Kafka consumer configuration arguments: "
            "Server: %s. "
            "Topic: %s. "
            "Security protocol: %s.",
            kwargs.get("bootstrap.servers"),
            self.topic,
            kwargs.get("security.protocol"),
        )
        self.kafka_prod = Producer(**kwargs)
        self.service_name = service_name

        LOG.info(
            "Sending status reports to Payload Tracker on topic %s as service %s",
            self.topic,
            self.service_name,
        )

    def _publish_status(self, input_msg, status, status_msg=None):
        """Send an status update to payload tracker topic."""
        request_id = input_msg.get("request_id")
        identity_field = input_msg.get("identity", {}).get("identity", {})
        org_id = identity_field.get("internal", {}).get("org_id")
        account = identity_field.get("account_number")
        cluster_name = input_msg.get("cluster_name", "")

        if request_id is None:
            LOG.warning("The received record doesn't contain a request_id. It won't be reported")
            return

        tracker_msg = {
            "service": self.service_name,
            "request_id": request_id,
            "status": status,
            "date": datetime.datetime.now().isoformat(),
        }

        if org_id:
            tracker_msg["org_id"] = org_id

        if account:
            tracker_msg["account"] = account

        if status_msg:
            tracker_msg["status_msg"] = status_msg

        self.kafka_prod.produce(self.topic, json.dumps(tracker_msg).encode("utf-8"))
        self.kafka_prod.poll(0)
        LOG.debug(
            "Payload Tracker update successfully sent for cluster %s: %s %s",
            cluster_name,
            request_id,
            status,
        )

    def on_recv(self, input_msg):
        """On received event handler."""
        self._publish_status(input_msg, "received")

    def on_process(self, input_msg, results):
        """On processing a new archive event handler."""
        self._publish_status(input_msg, "processing")

    def on_consumer_success(self, input_msg, broker, results):
        """On consumer success event handler."""
        self._publish_status(input_msg, "success")

    def on_consumer_failure(self, input_msg, exception):
        """On consumer failure event handler."""
        self._publish_status(input_msg, "error", str(exception))
