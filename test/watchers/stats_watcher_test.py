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

from unittest.mock import MagicMock, patch

import pytest
import time

from ccx_messaging.watchers.stats_watcher import StatsWatcher


_INVALID_PORTS = [None, "8000", 8000.0, 80, 70000]


@pytest.mark.parametrize("value", _INVALID_PORTS)
def test_stats_watcher_initialize_invalid_port(value):
    """Test passing invalid data types or values to the `StatsWatcher` initializer fails."""
    with pytest.raises((TypeError, PermissionError, OverflowError)):
        _ = StatsWatcher(value)


_VALID_PORTS = [dict(), {"prometheus_port": 9500}]


@pytest.mark.parametrize("value", _VALID_PORTS)
@patch("ccx_messaging.watchers.stats_watcher.start_http_server")
def test_stats_watcher_initialize(start_http_server_mock, value):
    """Test valid values in the initialize `StatsWatcher`."""
    StatsWatcher(**value)
    port = value.get("prometheus_port", 8000)  # 8000 is the default value
    start_http_server_mock.assert_called_with(port)


def check_initial_metrics_state(w):
    """Check that all metrics are initialized."""
    assert w._recv_total._value.get() == 0
    assert w._downloaded_total._value.get() == 0
    assert w._processed_total._value.get() == 0
    assert w._processed_timeout_total._value.get() == 0
    assert w._published_total._value.get() == 0
    assert w._failures_total._value.get() == 0
    assert w._not_handling_total._value.get() == 0


def init_timestamps(w):
    """Initialize all timestamps in watcher."""
    t = time.time()
    w._start_time = t
    w._downloaded_time = t
    w._processed_time = t
    w._published_time = t


def test_stats_watcher_on_recv():
    """Test the on_recv() method."""
    input_msg_mock = MagicMock()
    input_msg_mock.value = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8001)
    init_timestamps(w)

    # check that all metrics are initialized
    check_initial_metrics_state(w)

    # change metrics
    w.on_recv(input_msg_mock)

    # test new metrics values
    assert w._recv_total._value.get() == 1
    assert w._downloaded_total._value.get() == 0
    assert w._processed_total._value.get() == 0
    assert w._processed_timeout_total._value.get() == 0
    assert w._published_total._value.get() == 0
    assert w._failures_total._value.get() == 0
    assert w._not_handling_total._value.get() == 0


def test_stats_watcher_on_download():
    """Test the on_download() method."""

    # construct watcher object
    w = StatsWatcher(prometheus_port=8002)
    init_timestamps(w)

    # check that all metrics are initialized
    check_initial_metrics_state(w)

    # change metrics
    w.on_download("path")

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._downloaded_total._value.get() == 1
    assert w._processed_total._value.get() == 0
    assert w._processed_timeout_total._value.get() == 0
    assert w._published_total._value.get() == 0
    assert w._failures_total._value.get() == 0
    assert w._not_handling_total._value.get() == 0


def test_stats_watcher_on_process():
    """Test the on_process() method."""
    input_msg_mock = MagicMock()
    input_msg_mock.value = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8003)
    init_timestamps(w)

    # check that all metrics are initialized
    check_initial_metrics_state(w)

    # change metrics
    w.on_process(input_msg_mock, "{result}")

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._downloaded_total._value.get() == 0
    assert w._processed_total._value.get() == 1
    assert w._processed_timeout_total._value.get() == 0
    assert w._published_total._value.get() == 0
    assert w._failures_total._value.get() == 0
    assert w._not_handling_total._value.get() == 0


def test_stats_watcher_on_process_timeout():
    """Test the on_process_timeout() method."""

    # construct watcher object
    w = StatsWatcher(prometheus_port=8004)
    init_timestamps(w)

    # change metrics
    w.on_process_timeout()

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._downloaded_total._value.get() == 0
    assert w._processed_total._value.get() == 0
    assert w._processed_timeout_total._value.get() == 1
    assert w._published_total._value.get() == 0
    assert w._failures_total._value.get() == 0
    assert w._not_handling_total._value.get() == 0


def test_stats_watcher_on_consumer_success():
    """Test the on_consumer_success() method."""
    input_msg_mock = MagicMock()
    input_msg_mock.value = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8005)
    init_timestamps(w)

    # change metrics
    w.on_consumer_success(input_msg_mock, "broker", "{result}")

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._downloaded_total._value.get() == 0
    assert w._processed_total._value.get() == 0
    assert w._processed_timeout_total._value.get() == 0
    assert w._published_total._value.get() == 1
    assert w._failures_total._value.get() == 0
    assert w._not_handling_total._value.get() == 0


def test_stats_watcher_on_consumer_failure():
    """Test the on_consumer_failure() method."""
    input_msg_mock = MagicMock()
    input_msg_mock.value = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8006)
    init_timestamps(w)

    # change metrics
    w.on_consumer_failure(input_msg_mock, Exception("something"))

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._downloaded_total._value.get() == 0
    assert w._processed_total._value.get() == 0
    assert w._processed_timeout_total._value.get() == 0
    assert w._published_total._value.get() == 0
    assert w._failures_total._value.get() == 1
    assert w._not_handling_total._value.get() == 0

    # reset downloaded time
    w._downloaded_time = None

    # change metrics again
    w.on_consumer_failure(input_msg_mock, Exception("something"))

    # metric should change
    assert w._failures_total._value.get() == 2

    # reset processed time
    w._processed_time = None

    # change metrics again
    w.on_consumer_failure(input_msg_mock, Exception("something"))

    # metric should change again
    assert w._failures_total._value.get() == 3

    # now try this - downloaded time is not None and processed time is none
    w._downloaded_time = time.time()
    w._processed_time = None

    # change metrics again
    w.on_consumer_failure(input_msg_mock, Exception("something"))

    # metric should change again
    assert w._failures_total._value.get() == 4



def test_stats_watcher_on_not_handled():
    """Test the on_not_handled() method."""
    input_msg_mock = MagicMock()
    input_msg_mock.value = {"identity": {}}

    # construct watcher object
    w = StatsWatcher(prometheus_port=8007)
    init_timestamps(w)

    # change metrics
    w.on_not_handled(input_msg_mock)

    # test new metrics values
    assert w._recv_total._value.get() == 0
    assert w._downloaded_total._value.get() == 0
    assert w._processed_total._value.get() == 0
    assert w._processed_timeout_total._value.get() == 0
    assert w._published_total._value.get() == 0
    assert w._failures_total._value.get() == 0
    assert w._not_handling_total._value.get() == 1


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
