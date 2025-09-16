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

import pytest
from insights.formats.text import HumanReadableFormat

from ccx_messaging.engines.multiplexor_engine import MultiplexorEngine


def test_init():
    """Test the MultiplexorEngine initializer."""
    sut = MultiplexorEngine(HumanReadableFormat, [], None, 10)

    # just basic check
    assert isinstance(sut, MultiplexorEngine)


def test_process_no_extract():
    """Basic test for MultiplexorEngine."""
    sut = MultiplexorEngine(HumanReadableFormat, [], None, 10)
    sut.watchers = []
    sut.extract_tmp_dir = "not-exist"

    broker = None
    path = "not-exist"

    with pytest.raises(Exception):
        sut.process(broker, path)


def test_process_no_filters():
    """Basic test for MultiplexorEngine."""
    sut = MultiplexorEngine(HumanReadableFormat, [], None, 10)
    sut.watchers = []
    sut.extract_tmp_dir = ""

    broker = None
    path = "test/wrong_data.tar"  # this file should be marked as "DEFAULT"

    result = sut.process(broker, path)
    assert result == {"DEFAULT"}


def test_process_filters_no_match():
    """Basic test for MultiplexorEngine."""
    filters = {
        "non-existing-file": "SHOULDNT MARK",
    }
    sut = MultiplexorEngine(HumanReadableFormat, [], None, 10, filters=filters)
    sut.watchers = []
    sut.extract_tmp_dir = ""

    broker = None
    path = "test/wrong_data.tar"  # this file should be marked as "DEFAULT"

    result = sut.process(broker, path)
    assert result == {"DEFAULT"}


def test_process_filters_match():
    """Basic test for MultiplexorEngine."""
    expected_mark = "WORKLOAD_INFO"

    filters = {
        "data/config/workload_info.json": expected_mark,
    }
    sut = MultiplexorEngine(HumanReadableFormat, [], None, 10, filters=filters)
    sut.watchers = []
    sut.extract_tmp_dir = ""

    broker = None
    path = "test/wrong_data.tar"  # this file should be marked as "DEFAULT"

    result = sut.process(broker, path)
    assert result == {expected_mark}


def test_process_several_matches():
    """Basic test for SHAExtractorEngine."""
    filters = {
        "config": "IO",
        "config/workload_info.json": "WORKLOAD_INFO",
    }

    sut = MultiplexorEngine(HumanReadableFormat, [], None, 10, filters=filters)
    sut.watchers = []
    sut.extract_tmp_dir = ""

    broker = None
    path = "test/correct_data.tar"

    result = sut.process(broker, path)
    assert "IO" in result
    assert "WORKLOAD_INFO" in result
    assert len(result) == 2


def test_multiplexor_engine_tarfile_error():
    """Test MultiplexorEngine tarfile.ReadError handling."""
    import tarfile
    from unittest.mock import patch
    from ccx_messaging.error import CCXMessagingError

    sut = MultiplexorEngine(HumanReadableFormat, [], None, 10)
    sut.watchers = []
    sut.extract_tmp_dir = ""

    broker = None
    test_path = "/tmp/test_corrupted_archive.tar"

    # Mock tarfile.open to raise ReadError
    with patch("tarfile.open") as mock_open:
        mock_open.side_effect = tarfile.ReadError("file could not be opened successfully")

        # Test that CCXMessagingError is raised with correct format
        with pytest.raises(CCXMessagingError) as exc_info:
            sut.process(broker, test_path)

        # Verify the error structure
        error = exc_info.value
        assert str(error) == "ReadError reading the archive"
        assert error.additional_data is not None
        assert error.additional_data["archive_path"] == test_path
