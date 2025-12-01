# Copyright 2024 Red Hat Inc.
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


"""Unit tests for s3_upload_engine."""

import json
import os
import pytest
import tarfile
from unittest.mock import MagicMock, patch

from ccx_messaging.engines.s3_upload_engine import S3UploadEngine, extract_cluster_id
from ccx_messaging.error import CCXMessagingError
from ccx_messaging.utils.s3_uploader import S3Uploader
from ccx_messaging.watchers.stats_watcher import StatsWatcher


BROKER = {
    "cluster_id": "11111111-2222-3333-4444-555555555555",
    "org_id": "00000000",
    "original_path": "00000000/11111111-2222-3333-4444-555555555555/66666666666666-77777777777777777777777777777777",  # noqa: E501
    "year": "6666",
    "month": "66",
    "day": "66",
    "time": "666666",
    "hour": "66",
    "minute": "66",
    "second": "66",
    "id": "77777777777777777777777777777777",
}


BROKER2 = {
    "cluster_id": "22222222-3333-4444-5555-666666666666",
    "org_id": "00000000",
    "original_path": "00000000/22222222-3333-4444-5555-666666666666/77777777777777-88888888888888888888888888888888",  # noqa: E501
    "year": "7777",
    "month": "77",
    "day": "77",
    "time": "777777",
    "hour": "77",
    "minute": "77",
    "second": "77",
    "id": "88888888888888888888888888888888",
}


METADATA = {
    "path": "archives/compressed/11/11111111-2222-3333-4444-555555555555/666666/66/666666.tar.gz",
    "original_path": "00000000/11111111-2222-3333-4444-555555555555/66666666666666-77777777777777777777777777777777",  # noqa: E501
    "metadata": {
        "cluster_id": "11111111-2222-3333-4444-555555555555",
        "external_organization": "00000000",
    },
}


LOCAL_FILE_PATH = "file_path"
DEST_BUCKET = "bucket"


def test_init():
    """Test inicialization of engine."""
    engine = S3UploadEngine(
        None,
        access_key="test",
        secret_key="test",
        endpoint="https://s3.amazonaws.com",
    )
    assert engine is not None


def test_bad_key():
    """Test inicialization of engine with bad key."""
    with pytest.raises(TypeError):
        S3UploadEngine(
            None,
            access_key="",
            secret_key="",
            endpoint="https://s3.amazonaws.com",
        )


def test_bad_url():
    """Test inicialization of engine with bad URL."""
    with pytest.raises(ValueError):
        S3UploadEngine(
            None,
            access_key="test",
            secret_key="test",
            endpoint="BAD_URL",
        )


@patch("ccx_messaging.engines.s3_upload_engine.tarfile.open")
def test_engine_metadata(mock_tarfile):
    """Test of engines processing of metadata."""
    mock_tarfile.side_effect = tarfile.ReadError("not a tarfile")
    engine = S3UploadEngine(
        None,
        access_key="test",
        secret_key="test",
        endpoint="https://s3.amazonaws.com",
        dest_bucket=DEST_BUCKET,
        archives_path_prefix="archives/compressed",
    )
    engine.uploader = MagicMock()
    metadata = engine.process(BROKER, LOCAL_FILE_PATH)
    assert metadata == json.dumps(METADATA)


@patch("ccx_messaging.engines.s3_upload_engine.tarfile.open")
def test_engine_upload_file(mock_tarfile):
    """Test if upload_file metode was called with correct arguments."""
    mock_tarfile.side_effect = tarfile.ReadError("not a tarfile")
    engine = S3UploadEngine(
        None,
        access_key="test",
        secret_key="test",
        endpoint="https://s3.amazonaws.com",
        dest_bucket=DEST_BUCKET,
    )
    engine.uploader = MagicMock()
    S3Uploader.client = MagicMock()
    metadata = engine.process(BROKER2.copy(), LOCAL_FILE_PATH)
    metadata = json.loads(metadata)
    _, args, _ = engine.uploader.mock_calls[0]
    assert args[0] == LOCAL_FILE_PATH
    assert args[1] == DEST_BUCKET
    assert args[2] == metadata.get("path")


def test_uploader():
    """Test of uploader."""
    file = open(LOCAL_FILE_PATH, "a")
    file.write("test data")
    file.close()
    uploader = S3Uploader(access_key="test", secret_key="test", endpoint="https://s3.amazonaws.com")
    uploader.client.put_object = MagicMock()
    uploader.upload_file(path="file_path", bucket=DEST_BUCKET, file_name=METADATA.get("path"))
    os.remove(LOCAL_FILE_PATH)
    _, _, kwargs = uploader.client.put_object.mock_calls[0]
    assert kwargs.get("Bucket") == DEST_BUCKET
    assert kwargs.get("Key") == METADATA.get("path")


def test_uploader_no_existing_file():
    """Test uploading nonexistent file."""
    with pytest.raises(FileNotFoundError):
        uploader = S3Uploader(
            access_key="test", secret_key="test", endpoint="https://s3.amazonaws.com"
        )
        uploader.client.put_object = MagicMock()
        uploader.upload_file(path="file_path", bucket=DEST_BUCKET, file_name=METADATA.get("path"))


@patch("ccx_messaging.engines.s3_upload_engine.tarfile.open")
def test_path_using_timestamp(mock_tarfile):
    """Check path creation using timestamp elements."""
    mock_tarfile.side_effect = tarfile.ReadError("not a tarfile")
    engine = S3UploadEngine(
        None,
        access_key="test",
        secret_key="test",
        endpoint="https://s3.amazonaws.com",
        dest_bucket=DEST_BUCKET,
        archive_name_pattern="$year/$month/$day/$cluster_id-$id.tar.gz",
    )
    engine.uploader = MagicMock()
    S3Uploader.client = MagicMock()

    report = engine.process(BROKER2.copy(), LOCAL_FILE_PATH)
    report = json.loads(report)
    assert (
        report.get("path")
        == "7777/77/77/22222222-3333-4444-5555-666666666666-88888888888888888888888888888888.tar.gz"
    )


@patch("ccx_messaging.watchers.stats_watcher.start_http_server", lambda *args: None)
def test_archive_type_detection_with_prepopulated_cluster_id():
    """Test that archive type is detected even when cluster_id is pre-populated."""
    broker = BROKER.copy()
    broker["cluster_id"] = "11111111-2222-3333-4444-555555555555"  # Pre-populated

    engine = S3UploadEngine(
        None,
        access_key="test",
        secret_key="test",
        endpoint="https://s3.amazonaws.com",
        dest_bucket=DEST_BUCKET,
    )
    engine.uploader = MagicMock()

    # Attach StatsWatcher to verify archive type detection
    watcher = StatsWatcher(prometheus_port=9000)
    engine.watchers.append(watcher)

    # Process with real OLS archive
    engine.process(broker, "test/ols.tar")

    # Verify archive type was detected as "ols"
    assert watcher._archive_metadata["type"] == "ols"
    assert broker["cluster_id"] == "11111111-2222-3333-4444-555555555555"  # Unchanged


def test_cluster_id_extraction_when_none():
    """Test that cluster_id is extracted when broker has None."""
    broker = BROKER.copy()
    broker["cluster_id"] = None  # No cluster_id

    engine = S3UploadEngine(
        None,
        access_key="test",
        secret_key="test",
        endpoint="https://s3.amazonaws.com",
        dest_bucket=DEST_BUCKET,
    )
    engine.uploader = MagicMock()

    # Process with test OCP archive that has cluster_id in config/id
    engine.process(broker, "test/ocp_with_id.tar")

    # Verify cluster_id was extracted from the archive
    assert broker["cluster_id"] is not None
    assert len(broker["cluster_id"]) > 0
    # Verify the exact value and ensure no trailing newline
    assert broker["cluster_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def test_non_tarfile_handling():
    """Test that non-tarfile (like JSON) is handled gracefully."""
    broker = BROKER.copy()
    broker["cluster_id"] = "pre-populated-id"

    engine = S3UploadEngine(
        None,
        access_key="test",
        secret_key="test",
        endpoint="https://s3.amazonaws.com",
        dest_bucket=DEST_BUCKET,
    )
    engine.uploader = MagicMock()

    # Use this test file itself as a non-tarfile
    non_tarfile = __file__

    # Process non-tarfile - should not raise exception
    engine.process(broker, non_tarfile)

    # Verify it still uploaded (even though on_extract failed)
    assert engine.uploader.upload_file.called
    # Cluster ID should remain unchanged
    assert broker["cluster_id"] == "pre-populated-id"


def test_extract_cluster_id_from_non_tarfile():
    """Test that processing raises error for non-tarfile when cluster_id is None."""
    broker = BROKER.copy()
    broker["cluster_id"] = None  # cluster_id is None, so extraction will be attempted

    engine = S3UploadEngine(
        None,
        access_key="test",
        secret_key="test",
        endpoint="https://s3.amazonaws.com",
        dest_bucket=DEST_BUCKET,
    )
    engine.uploader = MagicMock()

    # Try to process a non-tarfile when cluster_id is None - should raise error
    with pytest.raises(CCXMessagingError, match="doesn't look as a tarfile"):
        engine.process(broker, __file__)


def test_extract_cluster_id_missing_config_id():
    """Test that extract_cluster_id raises error when tar doesn't have config/id."""
    # Use an archive without config/id (OLS archive doesn't have it)
    with tarfile.open("test/ols.tar") as tf:
        with pytest.raises(CCXMessagingError, match="doesn't contain cluster id"):
            extract_cluster_id(tf, "test/ols.tar")


def test_extract_cluster_id_success():
    """Test that extract_cluster_id successfully extracts cluster_id."""
    with tarfile.open("test/ocp_with_id.tar") as tf:
        cluster_id = extract_cluster_id(tf, "test/ocp_with_id.tar")
        assert cluster_id == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
