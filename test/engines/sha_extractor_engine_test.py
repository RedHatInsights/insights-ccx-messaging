# Copyright 2022 Red Hat, Inc
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

from ccx_messaging.engines.sha_extractor_engine import SHAExtractorEngine


def test_init():
    """Test the SHAExtractorEngine constructor."""
    e = SHAExtractorEngine(HumanReadableFormat)

    # just basic check
    assert e is not None


def test_process_no_extract():
    """Basic test for SHAExtractorEngine."""
    e = SHAExtractorEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = "not-exist"

    broker = None
    path = "not-exist"

    with pytest.raises(Exception):
        e.process(broker, path)


def test_process_extract_wrong_data():
    """Basic test for SHAExtractorEngine."""
    e = SHAExtractorEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = ""

    broker = None
    path = "test/wrong_data.tar"

    result = e.process(broker, path)
    assert result is None


def test_process_extract_correct_data():
    """Basic test for SHAExtractorEngine."""
    e = SHAExtractorEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = ""

    broker = None
    path = "test/correct_data.tar"

    result = e.process(broker, path)
    assert result is not None
