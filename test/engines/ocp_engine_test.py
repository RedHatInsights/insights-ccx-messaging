# Copyright 2024 Red Hat, Inc
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

"""Module for testing the engines module."""

from insights.formats.text import HumanReadableFormat
import pytest

from ccx_messaging.engines.ocp_engine import OCPEngine
from ccx_messaging.watchers.stats_watcher import StatsWatcher


def test_init():
    """Test the OCPEngine constructor."""
    e = OCPEngine(HumanReadableFormat)

    # just basic check
    assert e is not None


def test_process_no_extract():
    """Basic test for OCPEngine."""
    e = OCPEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = "not-exist"

    broker = None
    path = "not-exist"

    with pytest.raises(Exception):
        e.process(broker, path)


def test_process_extract_wrong_data():
    """Basic test for OCPEngine."""
    e = OCPEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = ""

    broker = None
    path = "test/wrong_data.tar"

    result = e.process(broker, path)
    assert result is not None


def test_process_extract_correct_data():
    """Basic test for OCPEngine."""
    e = OCPEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = ""

    broker = None
    path = "test/correct_data.tar"

    result = e.process(broker, path)
    assert result is not None


def test_process_extract_ols_archive():
    """Basic test for OCPEngine with an OLS file."""
    e = OCPEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = ""

    broker = None
    path = "test/ols.tar"

    result = e.process(broker, path)
    assert result == "{}"


def test_process_extract_rapid_recommendation_archive():
    """Basic test for OCPEngine with a rapid recommendation file."""
    e = OCPEngine(HumanReadableFormat)
    sw = StatsWatcher()
    e.watchers = [sw]
    e.extract_tmp_dir = ""

    broker = None
    path = "test/rapid-recommendations.tar.gz"

    result = e.process(broker, path)
    assert result != "{}"
    assert sw._gathering_conditions_remote_configuration_version.labels("1.1.0")._value.get() == 1
