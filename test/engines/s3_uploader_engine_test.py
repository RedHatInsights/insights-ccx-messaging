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
from unittest.mock import MagicMock

from ccx_messaging.engines.s3_upload_engine import S3UploadEngine
from ccx_messaging.utils.s3_uploader import S3Uploader


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


def test_engine_metadata():
    """Test of engines processing of metadata."""
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


def test_engine_upload_file():
    """Test if upload_file metode was called with correct arguments."""
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


def test_path_using_timestamp():
    """Check path creation using timestamp elements."""
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
