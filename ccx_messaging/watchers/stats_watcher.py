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
import os
import time

from prometheus_client import Counter, Histogram, start_http_server, REGISTRY

from ccx_messaging.watchers.consumer_watcher import ConsumerWatcher


LOG = logging.getLogger(__name__)
# Label to differentiate between OCP, OLS and HyperShift tarballs
ARCHIVE_TYPE_LABEL = "archive"
ARCHIVE_TYPE_VALUES = ["ocp", "hypershift", "ols"]


# pylint: disable=too-many-instance-attributes
class StatsWatcher(ConsumerWatcher):
    """A Watcher that stores different Prometheus `Counter`s."""

    def __init__(self, prometheus_port=8000):
        """Create the needed Counter objects and start serving Prometheus stats."""
        super().__init__()

        self._recv_total = Counter(
            "ccx_consumer_received_total", "Counter of received Kafka messages"
        )

        self._filtered_total = Counter(
            "ccx_consumer_filtered_total", "Counter of filtered Kafka messages"
        )

        self._downloaded_total = Histogram(
            "ccx_downloaded_total", "Histogram of the size of downloaded items"
        )

        self._archive_size = Histogram(
            "ccx_consumer_archive_size",
            "Histogram of the size of downloaded archive with archive type label",
            [ARCHIVE_TYPE_LABEL],
        )

        self._extracted_total = Counter(
            "ccx_consumer_extracted_total",
            "Counter of extracted archives",
            [ARCHIVE_TYPE_LABEL],
        )

        self._processed_total = Counter(
            "ccx_engine_processed_total",
            "Counter of files processed by the OCP Engine",
            [ARCHIVE_TYPE_LABEL],
        )

        self._published_total = Counter(
            "ccx_published_total",
            "Counter of reports successfully published",
            [ARCHIVE_TYPE_LABEL],
        )

        self._failures_total = Counter(
            "ccx_failures_total",
            "Counter of failures during the pipeline",
            [ARCHIVE_TYPE_LABEL],
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
            [ARCHIVE_TYPE_LABEL],
        )

        self._publish_duration = Histogram(
            "ccx_publish_duration_seconds",
            "Histogram of durations of publishing the OCP engine results",
            [ARCHIVE_TYPE_LABEL],
        )

        self._processed_timeout_total = Counter(
            "ccx_engine_processed_timeout_total",
            "Counter of timeouts while processing archives",
            [ARCHIVE_TYPE_LABEL],
        )

        self._start_time = None
        self._downloaded_time = None
        self._processed_time = None
        self._published_time = None

        # Archive type used in the metrics is set within on_extract, as we need
        # to extract the archive in order to know that information
        # TODO: Change to archive metadata dict, with archive type and archive size
        self._archive_metadata = {"type": "ocp", "size": 0}

        self._initialize_metrics_with_labels()

        start_http_server(prometheus_port)
        LOG.info("StatWatcher created and listening on port %s", prometheus_port)

    def on_recv(self, input_msg):
        """On received event handler."""
        self._recv_total.inc()

        self._start_time = time.time()
        self._reset_times()
        self._reset_archive_metadata()

    def on_filter(self):
        """On filter event handler."""
        self._filtered_total.inc()

    def on_extract(self, ctx, broker, extraction):
        """On extract event handler."""
        # Set archive_type label based on found file
        if os.path.exists(os.path.join(extraction.tmp_dir, "openshift_lightspeed.json")):
            self._archive_metadata["type"] = "ols"
        elif os.path.exists(os.path.join(extraction.tmp_dir, "config", "infrastructure.json")):
            self._archive_metadata["type"] = "hypershift"

        self._extracted_total.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).inc()

        self._archive_size.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).observe(
            self._archive_metadata["size"]
        )

    def on_download(self, path):
        """On downloaded event handler."""
        archive_size = os.path.getsize(path)

        self._archive_metadata["size"] = archive_size
        self._downloaded_total.observe(archive_size)

        self._downloaded_time = time.time()
        self._download_duration.observe(self._downloaded_time - self._start_time)

    def on_process(self, input_msg, results):
        """On processed event handler."""
        self._processed_total.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).inc()

        self._processed_time = time.time()
        self._process_duration.labels(
            **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
        ).observe(self._processed_time - self._downloaded_time)

    def on_process_timeout(self):
        """On process timeout event handler."""
        self._processed_timeout_total.labels(
            **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
        ).inc()

    def on_consumer_success(self, input_msg, broker, results):
        """On consumer success event handler."""
        self._published_total.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).inc()

        self._published_time = time.time()
        self._publish_duration.labels(
            **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
        ).observe(self._published_time - self._processed_time)

    def on_consumer_failure(self, input_msg, exception):
        """On consumer failure event handler."""
        self._failures_total.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).inc()

        if self._downloaded_time is None:
            self._download_duration.observe(time.time() - self._start_time)
            self._process_duration.labels(
                **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
            ).observe(0)
        elif self._processed_time is None:
            self._process_duration.labels(
                **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
            ).observe(time.time() - self._downloaded_time)

        self._publish_duration.labels(
            **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
        ).observe(0)

    def on_not_handled(self, input_msg):
        """On not handled messages success event handler."""
        self._not_handling_total.inc()

    def _reset_times(self):
        """Set all timestamps with the exception of start time to `None`."""
        self._downloaded_time = None
        self._processed_time = None
        self._published_time = None

    def _reset_archive_metadata(self):
        """Reset the cached archive metadata."""
        self._archive_metadata["type"] = "ocp"
        self._archive_metadata["size"] = 0

    def _initialize_metrics_with_labels(self):
        """Initialize Prometheus metrics that have at least one label.

        Metrics with labels are not initialized when declared, because the Prometheus
        client can’t know what values the label can have. This is therefore needed in
        order to initialize them.
        """
        for val in ARCHIVE_TYPE_VALUES:
            self._extracted_total.labels(val)
            self._archive_size.labels(val)
            self._processed_total.labels(val)
            self._published_total.labels(val)
            self._failures_total.labels(val)
            self._process_duration.labels(val)
            self._publish_duration.labels(val)
            self._processed_timeout_total.labels(val)

    def __del__(self):
        """Destructor for handling counters unregistering."""
        REGISTRY.unregister(self._recv_total)
        REGISTRY.unregister(self._filtered_total)
        REGISTRY.unregister(self._downloaded_total)
        REGISTRY.unregister(self._archive_size)
        REGISTRY.unregister(self._extracted_total)
        REGISTRY.unregister(self._processed_total)
        REGISTRY.unregister(self._published_total)
        REGISTRY.unregister(self._failures_total)
        REGISTRY.unregister(self._not_handling_total)
        REGISTRY.unregister(self._download_duration)
        REGISTRY.unregister(self._process_duration)
        REGISTRY.unregister(self._publish_duration)
        REGISTRY.unregister(self._processed_timeout_total)
