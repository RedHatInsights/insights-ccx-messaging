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

from pythonjsonlogger import jsonlogger
import pytest

from ccx_messaging.engines.sha_extractor_engine import SHAExtractorEngine

def test_init():
    """Test the SHAExtractorEngine constructor."""

    formatter = jsonlogger
    e = SHAExtractorEngine(formatter)

    # just basic check
    assert e is not None


def test_process():
    """Basic test for SHAExtractorEngine."""

    formatter = jsonlogger
    e = SHAExtractorEngine(formatter)
    e.watchers = []
    e.extract_tmp_dir = ""

    broker = None
    path = ""

    with pytest.raises(Exception):
        e.process(broker, path)
