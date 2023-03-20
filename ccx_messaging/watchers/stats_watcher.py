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

"""Module including instrumentation to expose retrieved stats to Prometheus."""

import logging
import time

from prometheus_client import Counter, Histogram, start_http_server, REGISTRY

from ccx_messaging.watchers.consumer_watcher import ConsumerWatcher


LOG = logging.getLogger(__name__)


# pylint: disable=too-many-instance-attributes
class StatsWatcher(ConsumerWatcher):
    """A Watcher that stores different Prometheus `Counter`s."""

    def __init__(self, prometheus_port=8000):
        """Create the needed Counter objects and start serving Prometheus stats."""
        super().__init__()

        self._recv_total = Counter(
            "ccx_consumer_received_total", "Counter of received Kafka messages"
        )

        self._downloaded_total = Counter("ccx_downloaded_total", "Counter of downloaded items")

        self._processed_total = Counter(
            "ccx_engine_processed_total", "Counter of files processed by the OCP Engine"
        )

        self._published_total = Counter(
            "ccx_published_total", "Counter of reports successfully published"
        )

        self._failures_total = Counter(
            "ccx_failures_total", "Counter of failures during the pipeline"
        )

        self._not_handling_total = Counter(
            "ccx_not_handled_total",
            "Counter of received elements that are not handled by the pipeline",
        )

        self._download_duration = Histogram(
            "ccx_download_duration_seconds", "Histogram of archive download durations"
        )

        self._process_duration = Histogram(
            "ccx_process_duration_seconds",
            "Histogram of durations of processing archives by the OCP engine",
        )

        self._publish_duration = Histogram(
            "ccx_publish_duration_seconds",
            "Histogram of durations of publishing the OCP engine results",
        )

        self._processed_timeout_total = Counter(
            "ccx_engine_processed_timeout_total",
            "Counter of timeouts while processing archives",
        )

        self._start_time = None
        self._downloaded_time = None
        self._processed_time = None
        self._published_time = None

        start_http_server(prometheus_port)
        LOG.info("StatWatcher created and listening on port %s", prometheus_port)

    def on_recv(self, input_msg):
        """On received event handler."""
        self._recv_total.inc()

        self._start_time = time.time()
        self._reset_times()

    def on_download(self, path):
        """On downloaded event handler."""
        self._downloaded_total.inc()

        self._downloaded_time = time.time()
        self._download_duration.observe(self._downloaded_time - self._start_time)

    def on_process(self, input_msg, results):
        """On processed event handler."""
        self._processed_total.inc()

        self._processed_time = time.time()
        self._process_duration.observe(self._processed_time - self._downloaded_time)

    def on_process_timeout(self):
        """On process timeout event handler."""
        self._processed_timeout_total.inc()

    def on_consumer_success(self, input_msg, broker, results):
        """On consumer success event handler."""
        self._published_total.inc()

        self._published_time = time.time()
        self._publish_duration.observe(self._published_time - self._processed_time)

    def on_consumer_failure(self, input_msg, exception):
        """On consumer failure event handler."""
        self._failures_total.inc()

        if self._downloaded_time is None:
            self._download_duration.observe(time.time() - self._start_time)
            self._process_duration.observe(0)
        elif self._processed_time is None:
            self._process_duration.observe(time.time() - self._downloaded_time)

        self._publish_duration.observe(0)

    def on_not_handled(self, input_msg):
        """On not handled messages success event handler."""
        self._not_handling_total.inc()

    def _reset_times(self):
        """Set all timestamps with the exception of start time to `None`."""
        self._downloaded_time = None
        self._processed_time = None
        self._published_time = None

    def __del__(self):
        """Destructor for handling counters unregistering."""
        REGISTRY.unregister(self._recv_total)
        REGISTRY.unregister(self._downloaded_total)
        REGISTRY.unregister(self._processed_total)
        REGISTRY.unregister(self._published_total)
        REGISTRY.unregister(self._failures_total)
        REGISTRY.unregister(self._not_handling_total)
        REGISTRY.unregister(self._download_duration)
        REGISTRY.unregister(self._process_duration)
        REGISTRY.unregister(self._publish_duration)
        REGISTRY.unregister(self._processed_timeout_total)
