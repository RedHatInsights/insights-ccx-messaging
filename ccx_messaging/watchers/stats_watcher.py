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
import tarfile
import time
import json

from insights.core.archives import TarExtractor, ZipExtractor
from insights_messaging.watchers import EngineWatcher
from prometheus_client import Counter, Histogram, start_http_server, REGISTRY

from ccx_messaging.watchers.consumer_watcher import ConsumerWatcher


LOG = logging.getLogger(__name__)
# Label to differentiate between OCP, OLS and HyperShift tarballs
ARCHIVE_TYPE_LABEL = "archive"
ARCHIVE_TYPE_VALUES = ["ocp", "hypershift", "ols"]
IO_GATHERING_REMOTE_CONFIG_LABEL = "version"
RECV_TOTAL = Counter(
            "ccx_consumer_received_total", "Counter of received Kafka messages"
        )
FILTERED_TOTAL = Counter(
            "ccx_consumer_filtered_total", "Counter of filtered Kafka messages"
        )
DOWNLOADED_TOTAL = Histogram(
            "ccx_downloaded_total", "Histogram of the size of downloaded items"
        )
ARCHIVE_SIZE = Histogram(
    "ccx_consumer_archive_size",
    "Histogram of the size of downloaded archive with archive type label",
    [ARCHIVE_TYPE_LABEL],
)

EXTRACTED_TOTAL = Counter(
    "ccx_consumer_extracted_total",
    "Counter of extracted archives",
    [ARCHIVE_TYPE_LABEL],
)

PROCESSED_TOTAL = Counter(
    "ccx_engine_processed_total",
    "Counter of files processed by the OCP Engine",
    [ARCHIVE_TYPE_LABEL],
)

PUBLISHED_TOTAL = Counter(
    "ccx_published_total",
    "Counter of reports successfully published",
    [ARCHIVE_TYPE_LABEL],
)

FAILURES_TOTAL = Counter(
    "ccx_failures_total",
    "Counter of failures during the pipeline",
    [ARCHIVE_TYPE_LABEL],
)

NOT_HANDLING_TOTAL = Counter(
    "ccx_not_handled_total",
    "Counter of received elements that are not handled by the pipeline",
)

DOWNLOAD_DURATION = Histogram(
    "ccx_download_duration_seconds", "Histogram of archive download durations"
)

PROCESS_DURATION = Histogram(
    "ccx_process_duration_seconds",
    "Histogram of durations of processing archives by the OCP engine",
    [ARCHIVE_TYPE_LABEL],
)

PUBLISH_DURATION = Histogram(
    "ccx_publish_duration_seconds",
    "Histogram of durations of publishing the OCP engine results",
    [ARCHIVE_TYPE_LABEL],
)

PROCESSED_TIMEOUT_TOTAL = Counter(
    "ccx_engine_processed_timeout_total",
    "Counter of timeouts while processing archives",
    [ARCHIVE_TYPE_LABEL],
)

GATHERING_CONDITIONS_REMOTE_CONFIGURATION_VERSION = Counter(
    "gathering_conditions_remote_configuration_version",
    "Counter of times a given configuration is seen",
    [IO_GATHERING_REMOTE_CONFIG_LABEL],
)
# pylint: disable=too-many-instance-attributes
class StatsWatcher(ConsumerWatcher, EngineWatcher):
    """A Watcher that stores different Prometheus `Counter`s."""

    def __init__(self, prometheus_port=8000):
        """Create the needed Counter objects and start serving Prometheus stats."""
        super().__init__()
        self._start_time = None
        self._downloaded_time = None
        self._processed_time = None
        self._published_time = None

        # Archive type used in the metrics is set within on_extract, as we need
        # to extract the archive in order to know that information
        # TODO: Change to archive metadata dict, with archive type and archive size
        self._archive_metadata = {"type": "ocp", "size": 0, "s3_path": ""}

        self._initialize_metrics_with_labels()

        start_http_server(prometheus_port)
        LOG.info("StatWatcher created and listening on port %s", prometheus_port)

    def on_recv(self, input_msg):
        """On received event handler."""
        LOG.debug("Receiving 'on_recv' callback")
        RECV_TOTAL.inc()

        self._start_time = time.time()
        self._reset_times()
        self._reset_archive_metadata()

        if "path" in input_msg:
            self._archive_metadata["s3_path"] = input_msg["path"]
        elif "url" in input_msg:
            # this means we are consuming directly from ingress (external
            # pipeline), so printing the URL would include some short-live
            # credentials used to access the archive. It's a risk to log this
            pass
        else:
            LOG.error(f"message has no S3 path: {input_msg}")

    def on_filter(self):
        """On filter event handler."""
        LOG.debug("Receiving 'on_filter' callback")
        FILTERED_TOTAL.inc()

    def on_extract(
        self, ctx, broker, extraction: tarfile.TarFile | TarExtractor | ZipExtractor
    ) -> None:
        """On extract event handler for extractor using engines."""
        LOG.debug("Receiving 'on_extract' callback")

        if isinstance(extraction, tarfile.TarFile):
            self.on_extract_with_tarfile(extraction)

        else:
            self.on_extract_with_extractor(extraction)

    def on_extract_with_tarfile(self, extraction: tarfile.TarFile) -> None:
        """On extract event handler for engines using tarfile."""
        tarfile_contents = extraction.getnames()

        if "openshift_lightspeed.json" in tarfile_contents:
            self._archive_metadata["type"] = "ols"
        elif os.path.join("config", "infrastructure.json") in tarfile_contents:
            self._archive_metadata["type"] = "hypershift"

    def on_extract_with_extractor(self, extraction: TarExtractor | ZipExtractor) -> None:
        """On extract event handler for engines using extractor."""
        # Set archive_type label based on found file
        if os.path.exists(os.path.join(extraction.tmp_dir, "openshift_lightspeed.json")):
            self._archive_metadata["type"] = "ols"
        elif os.path.exists(os.path.join(extraction.tmp_dir, "config", "infrastructure.json")):
            self._archive_metadata["type"] = "hypershift"

        EXTRACTED_TOTAL.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).inc()

        ARCHIVE_SIZE.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).observe(
            self._archive_metadata["size"]
        )

        # Set the IO remote configuration version
        remote_config_path = os.path.join(
            extraction.tmp_dir, "insights-operator", "remote-configuration.json"
        )
        try:
            with open(remote_config_path, "rb") as f:
                data = json.load(f)
                version = data["version"]
                self._gathering_conditions_remote_configuration_version.labels(version).inc()
        except FileNotFoundError:
            LOG.debug("this archive didn't use remote-configurations")
        except KeyError:
            LOG.warning(
                f"archive {self._archive_metadata['s3_path']} doesn't have "
                "a version in the remote-configuration"
            )
            self._gathering_conditions_remote_configuration_version.labels("None").inc()
        except Exception as e:
            LOG.error("cannot read remote-configuration.json", exc_info=e)

    def on_download(self, path):
        """On downloaded event handler."""
        LOG.debug("Receiving 'on_download' callback")
        archive_size = os.path.getsize(path)

        self._archive_metadata["size"] = archive_size
        DOWNLOADED_TOTAL.observe(archive_size)

        self._downloaded_time = time.time()
        DOWNLOAD_DURATION.observe(self._downloaded_time - self._start_time)

    def on_process(self, input_msg, results):
        """On processed event handler."""
        LOG.debug("Receiving 'on_process' callback")
        PROCESSED_TOTAL.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).inc()

        self._processed_time = time.time()
        PROCESS_DURATION.labels(
            **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
        ).observe(self._processed_time - self._downloaded_time)

    def on_process_timeout(self):
        """On process timeout event handler."""
        LOG.debug("Receiving 'on_process_timeout' callback")
        PROCESSED_TIMEOUT_TOTAL.labels(
            **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
        ).inc()

    def on_consumer_success(self, input_msg, broker, results):
        """On consumer success event handler."""
        LOG.debug("Receiving 'on_consumer_success' callback")
        PUBLISHED_TOTAL.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).inc()

        self._published_time = time.time()
        PUBLISH_DURATION.labels(
            **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
        ).observe(self._published_time - self._processed_time)

    def on_consumer_failure(self, input_msg, exception):
        """On consumer failure event handler."""
        LOG.debug("Receiving 'on_consumer_failure' callback")
        FAILURES_TOTAL.labels(**{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}).inc()

        if self._downloaded_time is None:
            DOWNLOAD_DURATION.observe(time.time() - self._start_time)
            PROCESS_DURATION.labels(
                **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
            ).observe(0)
        elif self._processed_time is None:
            PROCESS_DURATION.labels(
                **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
            ).observe(time.time() - self._downloaded_time)

        PUBLISH_DURATION.labels(
            **{ARCHIVE_TYPE_LABEL: self._archive_metadata["type"]}
        ).observe(0)

    def on_not_handled(self, input_msg):
        """On not handled messages success event handler."""
        LOG.debug("Receiving 'on_not_handled' callback")
        NOT_HANDLING_TOTAL.inc()

    def _reset_times(self):
        """Set all timestamps with the exception of start time to `None`."""
        self._downloaded_time = None
        self._processed_time = None
        self._published_time = None

    def _reset_archive_metadata(self):
        """Reset the cached archive metadata."""
        self._archive_metadata["type"] = "ocp"
        self._archive_metadata["size"] = 0
        self._archive_metadata["s3_path"] = ""

    def _initialize_metrics_with_labels(self):
        """Initialize Prometheus metrics that have at least one label.

        Metrics with labels are not initialized when declared, because the Prometheus
        client can’t know what values the label can have. This is therefore needed in
        order to initialize them.
        """
        for val in ARCHIVE_TYPE_VALUES:
            EXTRACTED_TOTAL.labels(val)
            ARCHIVE_SIZE.labels(val)
            PROCESSED_TOTAL.labels(val)
            PUBLISHED_TOTAL.labels(val)
            FAILURES_TOTAL.labels(val)
            PROCESS_DURATION.labels(val)
            PUBLISH_DURATION.labels(val)
            PROCESSED_TIMEOUT_TOTAL.labels(val)
