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

from unittest.mock import MagicMock, mock_open, patch

import pytest

from ccx_messaging.watchers.cluster_id_watcher import ClusterIdWatcher


def _mock_input_msg():
    # pylint: disable=too-few-public-methods
    class InputMessageDouble:
        """Test double for InputMessage."""

        value = dict()

    return InputMessageDouble()


def test_cluster_id_watcher_unordered_events(caplog):
    """Test that an extraction event is received without a receiving one ."""
    sut = ClusterIdWatcher()
    sut.on_extract(None, None, None)

    assert sut.last_record is None
    assert "Unexpected data flow" in caplog.text


def test_cluster_id_watcher_file_not_exist(caplog):
    """Test that the extracted archive doesn't contain a cluster id file."""
    input_msg_mock = MagicMock()
    input_msg_mock.value = {"identity": {}}

    extraction_mock = MagicMock()
    extraction_mock.tmp_dir = "/tmp/to/non/existing/file/over/the/filesystem"

    sut = ClusterIdWatcher()
    sut.on_recv(input_msg_mock)

    sut.on_extract(None, None, extraction_mock)

    assert input_msg_mock.value["ClusterName"] is None
    assert "The archive doesn't contain a valid Cluster Id file" in caplog.text


_INCORRECT_UUIDS = [
    "0",
    "---//---",
    "hhhh-bbbb-cccc-dddd-000000000000",
]


@pytest.mark.parametrize("value", _INCORRECT_UUIDS)
def test_cluster_id_watcher_bad_content(caplog, value):
    """Test that the id file contains non UUID values."""
    input_msg_mock = MagicMock()
    input_msg_mock.value = {"identity": {}}

    extraction_mock = MagicMock()
    extraction_mock.tmp_dir = "/tmp/mock/path"

    sut = ClusterIdWatcher()
    sut.on_recv(input_msg_mock)

    with patch("builtins.open", mock_open(read_data=value)):
        sut.on_extract(None, None, extraction_mock)
        assert input_msg_mock.value["ClusterName"] is None
        assert "The cluster id is not an UUID" in caplog.text


def test_cluster_id_watcher_ok(caplog):
    """Test that the id file contains non UUID values."""
    input_msg_mock = MagicMock()
    input_msg_mock.value = {"identity": {}}

    extraction_mock = MagicMock()
    extraction_mock.tmp_dir = "/tmp/mock/path"

    sut = ClusterIdWatcher()
    sut.on_recv(input_msg_mock)

    uuid_value = "aaaaaaaa-bbbb-cccc-dddd-000000000000"

    with patch("builtins.open", mock_open(read_data=uuid_value)):
        sut.on_extract(None, None, extraction_mock)
        assert input_msg_mock.value["ClusterName"] == uuid_value
        assert len(caplog.records) == 0

        sut.last_record = MagicMock()
        sut.last_record.value = {"ClusterName": uuid_value}

        # now the cluster name should be set already
        # so the method ends w/o opening the file
        sut.on_extract(None, None, extraction_mock)
        assert input_msg_mock.value["ClusterName"] == uuid_value
