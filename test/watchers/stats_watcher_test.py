# Copyright 2020 Red Hat, Inc
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

from unittest.mock import patch

import pytest

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
