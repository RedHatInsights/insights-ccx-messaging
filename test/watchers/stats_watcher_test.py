# Copyright 2020, 2021, 2022 Red Hat, Inc
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

"""Module containing unit tests for the `ConsumerWatcher` class."""

import time
from tarfile import TarFile
from unittest.mock import MagicMock, patch

import pytest
from insights.core.archives import TarExtractor

from ccx_messaging.watchers.stats_watcher import (
    StatsWatcher,
    ARCHIVE_TYPE_LABEL,
    ARCHIVE_TYPE_VALUES,
)


_INVALID_PORTS = [None, "8000", 8000.0, 70000]


@pytest.fixture(params=ARCHIVE_TYPE_VALUES)
def label_value(request):
    """Set the label value for the running test."""
    return request.param


@pytest.mark.parametrize("value", _INVALID_PORTS)
def test_stats_watcher_initialize_invalid_port(value):
    """Test passing invalid data types or values to the `StatsWatcher` initializer fails."""
    with pytest.raises((TypeError, PermissionError, OverflowError, OSError)):
        _ = StatsWatcher(value)


_VALID_PORTS = [{}, {"prometheus_port": 9500}, {"prometheus_port": 80}]


@pytest.mark.parametrize("value", _VALID_PORTS)
@patch("ccx_messaging.watchers.stats_watcher.start_http_server")
def test_stats_watcher_initialize(start_http_server_mock, value):
    """Test valid values in the initialize `StatsWatcher`."""
    StatsWatcher(**value)
    port = value.get("prometheus_port", 8000)  # 8000 is the default value
    start_http_server_mock.assert_called_with(port)


def check_initial_metrics_state(w):
    """Check that all metrics are initialized."""
    for value in ARCHIVE_TYPE_VALUES:
        assert w._recv_total._value.get() == 0
        assert w._filtered_total._value.get() == 0
        assert w._extracted_total.labels(**{ARCHIVE_TYPE_LABEL: value})._value.get() == 0
        assert w._processed_total.labels(**{ARCHIVE_TYPE_LABEL: value})._value.get() == 0
        assert w._processed_timeout_total.labels(**{ARCHIVE_TYPE_LABEL: value})._value.get() == 0
        assert w._published_total.labels(**{ARCHIVE_TYPE_LABEL: value})._value.get() == 0
        assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: value})._value.get() == 0
        assert w._not_handling_total._value.get() == 0
        # Check initial values of histogram metrics
        assert w._downloaded_total._sum.get() == 0
        assert w._download_duration._sum.get() == 0
        assert w._process_duration.labels(**{ARCHIVE_TYPE_LABEL: value})._sum.get() == 0
        assert w._publish_duration.labels(**{ARCHIVE_TYPE_LABEL: value})._sum.get() == 0


def init_timestamps(w):
    """Initialize all timestamps in watcher."""
    t = time.time()
    w._start_time = t
    w._downloaded_time = t
    w._processed_time = t
    w._published_time = t


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_stats_watcher_on_recv(label_value):
    """Test the on_recv() method."""
    input_msg = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8001)
    init_timestamps(w)
    w._archive_metadata["type"] = label_value

    # check that all metrics are initialized
    check_initial_metrics_state(w)

    # change metrics
    w.on_recv(input_msg)

    # test new metrics values
    assert w._recv_total._value.get() == 1
    assert w._filtered_total._value.get() == 0
    assert len(w._downloaded_total._labelvalues) == 0
    assert w._processed_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._processed_timeout_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._published_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._not_handling_total._value.get() == 0


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_stats_watcher_on_filter(label_value):
    """Test the on_filter() method."""
    # construct watcher object
    w = StatsWatcher(prometheus_port=8001)
    init_timestamps(w)
    w._archive_metadata["type"] = label_value

    # check that all metrics are initialized
    check_initial_metrics_state(w)

    # change metrics
    w.on_filter()

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._filtered_total._value.get() == 1
    assert w._downloaded_total._sum.get() == 0
    assert w._processed_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._processed_timeout_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._published_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._not_handling_total._value.get() == 0


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_stats_watcher_on_download(label_value):
    """Test the on_download() method."""
    # construct watcher object
    w = StatsWatcher(prometheus_port=8002)
    init_timestamps(w)
    w._archive_metadata["type"] = label_value

    # check that all metrics are initialized
    check_initial_metrics_state(w)

    # change metrics
    with patch("ccx_messaging.watchers.stats_watcher.os.path.getsize", lambda x: 100):
        w.on_download("path")

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._filtered_total._value.get() == 0
    assert w._downloaded_total._sum.get() == 100
    assert w._processed_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._processed_timeout_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._published_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._not_handling_total._value.get() == 0

    assert w._archive_metadata["size"] == 100


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_stats_watcher_on_process(label_value):
    """Test the on_process() method."""
    input_msg = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8003)
    init_timestamps(w)
    w._archive_metadata["type"] = label_value

    # check that all metrics are initialized
    check_initial_metrics_state(w)

    # change metrics
    w.on_process(input_msg, "{result}")

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._filtered_total._value.get() == 0
    assert w._downloaded_total._sum.get() == 0
    assert w._processed_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 1
    assert w._processed_timeout_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._published_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._not_handling_total._value.get() == 0


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_stats_watcher_on_process_timeout(label_value):
    """Test the on_process_timeout() method."""
    # construct watcher object
    w = StatsWatcher(prometheus_port=8004)
    init_timestamps(w)
    w._archive_metadata["type"] = label_value

    # change metrics
    w.on_process_timeout()

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._filtered_total._value.get() == 0
    assert w._downloaded_total._sum.get() == 0
    assert w._processed_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._processed_timeout_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 1
    assert w._published_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._not_handling_total._value.get() == 0


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_stats_watcher_on_consumer_success(label_value):
    """Test the on_consumer_success() method."""
    input_msg = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8005)
    init_timestamps(w)
    w._archive_metadata["type"] = label_value

    # change metrics
    w.on_consumer_success(input_msg, "broker", "{result}")

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._filtered_total._value.get() == 0
    assert w._downloaded_total._sum.get() == 0
    assert w._processed_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._processed_timeout_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._published_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 1
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._not_handling_total._value.get() == 0


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_stats_watcher_on_consumer_failure(label_value):
    """Test the on_consumer_failure() method."""
    input_msg = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8006)
    init_timestamps(w)
    w._archive_metadata["type"] = label_value

    # change metrics
    w.on_consumer_failure(input_msg, Exception("something"))

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._filtered_total._value.get() == 0
    assert w._downloaded_total._sum.get() == 0
    assert w._processed_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._processed_timeout_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._published_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 1
    assert w._not_handling_total._value.get() == 0

    # reset downloaded time
    w._downloaded_time = None

    # change metrics again
    w.on_consumer_failure(input_msg, Exception("something"))

    # metric should change
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 2

    # reset processed time
    w._processed_time = None

    # change metrics again
    w.on_consumer_failure(input_msg, Exception("something"))

    # metric should change again
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 3

    # now try this - downloaded time is not None and processed time is none
    w._downloaded_time = time.time()
    w._processed_time = None

    # change metrics again
    w.on_consumer_failure(input_msg, Exception("something"))

    # metric should change again
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 4


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_stats_watcher_on_not_handled(label_value):
    """Test the on_not_handled() method."""
    input_msg = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8007)
    init_timestamps(w)
    w._archive_metadata["type"] = label_value

    # change metrics
    w.on_not_handled(input_msg)

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._filtered_total._value.get() == 0
    assert w._downloaded_total._sum.get() == 0
    assert w._processed_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._processed_timeout_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._published_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._failures_total.labels(**{ARCHIVE_TYPE_LABEL: label_value})._value.get() == 0
    assert w._not_handling_total._value.get() == 1


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_reset_times():
    """Test the method _reset_times()."""
    # construct watcher object
    w = StatsWatcher(prometheus_port=8008)
    init_timestamps(w)

    assert w._start_time is not None
    assert w._downloaded_time is not None
    assert w._processed_time is not None
    assert w._published_time is not None

    w._reset_times()

    assert w._start_time is not None
    assert w._downloaded_time is None
    assert w._processed_time is None
    assert w._published_time is None


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_reset_archive_metadata(label_value):
    """Test the method _reset_times()."""
    # construct watcher object
    w = StatsWatcher(prometheus_port=8009)
    w._archive_metadata["type"] = label_value

    w._reset_archive_metadata()

    assert w._archive_metadata["type"] == "ocp"


ON_EXTRACT_WITH_EXTRACTOR_IDENTIFIERS = [
    ("ols", [True, False]),
    ("hypershift", [False, True]),
    ("ocp", [False, False]),
]


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
@pytest.mark.parametrize("type_, exists_side_effect", ON_EXTRACT_WITH_EXTRACTOR_IDENTIFIERS)
def test_on_extract_with_extractor(type_, exists_side_effect):
    """Test the method on_extract."""
    # construct watcher object
    w = StatsWatcher(prometheus_port=8009)

    extraction_mock = MagicMock(spec=TarExtractor)
    extraction_mock.tmp_dir = ""

    with patch("os.path.exists") as exists_mock:
        exists_mock.side_effect = exists_side_effect

        w.on_extract(None, None, extraction_mock)
        assert w._archive_metadata["type"] == type_


ON_EXTRACT_WITH_TARFILE_IDENTIFIERS = [
    ("ols", ["openshift_lightspeed.json"]),
    ("hypershift", ["config/id", "config/infrastructure.json"]),
    ("ocp", ["config/id"]),
]


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
@pytest.mark.parametrize("type_, file_content", ON_EXTRACT_WITH_TARFILE_IDENTIFIERS)
def test_on_extract_with_tarfile(type_, file_content):
    """Test the method on_extract."""
    # construct watcher object
    w = StatsWatcher(prometheus_port=8009)

    extraction_mock = MagicMock(spec=TarFile)
    extraction_mock.getnames.return_value = file_content
    w.on_extract(None, None, extraction_mock)
    assert w._archive_metadata["type"] == type_
