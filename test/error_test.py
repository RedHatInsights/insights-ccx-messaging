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

"""Tests for error classes."""

from ccx_messaging.error import CCXMessagingError


input_msg = {
    "topic": "topic name",
    "partition": "partition name",
    "offset": 1234,
    "url": "any/url",
    "identity": {"identity": {"internal": {"org_id": "12345678"}}},
    "timestamp": "2020-01-23T16:15:59.478901889Z",
    "cluster_name": "clusterName",
}


def test_error_formatting():
    """Test the error formatting."""
    err = CCXMessagingError("CCXMessagingError")
    assert err is not None

    fmt = err.format(input_msg)
    expected = "Status: Error; Topic: topic name; Cause: CCXMessagingError"
    assert fmt == expected


def test_error_without_additional_data():
    """Test CCXMessagingError without additional_data (backwards compatibility)."""
    err = CCXMessagingError("Test error message")

    assert err is not None
    assert str(err) == "Test error message"
    assert err.additional_data is None


def test_error_with_additional_data():
    """Test CCXMessagingError with additional_data parameter."""
    test_data = {"archive_path": "/tmp/test.tar", "cluster_id": "test-cluster"}
    err = CCXMessagingError("Test error with data", additional_data=test_data)

    assert err is not None
    assert str(err) == "Test error with data"
    assert err.additional_data == test_data
    assert err.additional_data["archive_path"] == "/tmp/test.tar"
    assert err.additional_data["cluster_id"] == "test-cluster"


def test_error_with_empty_additional_data():
    """Test CCXMessagingError with empty dict as additional_data."""
    err = CCXMessagingError("Test error", additional_data={})

    assert err is not None
    assert str(err) == "Test error"
    assert err.additional_data == {}
