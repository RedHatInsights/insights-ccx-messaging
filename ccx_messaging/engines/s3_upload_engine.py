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

"""S3 Engine Class and related functions."""

import json
import re
import logging

from insights_messaging.engine import Engine
from ccx_messaging.utils.s3_uploader import S3Uploader
from ccx_messaging.error import CCXMessagingError


S3_ARCHIVE_PATTERN = re.compile(r"^([0-9]+)\/([0-9,a-z,-]{36})\/([0-9]{14})-[a-z,A-Z,0-9]*$")
LOG = logging.getLogger(__name__)


def extract_org_id(file_path):
    """Extract organisation ID from s3 path."""
    search = re.search(S3_ARCHIVE_PATTERN, file_path)
    if not search:
        LOG.warning(
            "Unrecognized archive path. Can't search any '%s' pattern for '%s' path.",
            S3_ARCHIVE_PATTERN,
            file_path,
        )
        return
    return search.group(1)


def compute_target_path(file_path, prefix):
    """Compute S3 target path from the source path.

    Target path is in archives/compressed/$ORG_ID/$CLUSTER_ID/$YEAR$MONTH/$DAY/$TIME.tar.gz format.
    """
    search = search = re.search(S3_ARCHIVE_PATTERN, file_path)
    if not search:
        LOG.warning(
            "Unrecognized archive path. Can't search any '%s' pattern for '%s' path.",
            S3_ARCHIVE_PATTERN,
            file_path,
        )
        raise CCXMessagingError("Unable to compute target path")

    cluster_id = f"{search.group(2)}"
    archive = search.group(3)
    datetime = archive.split(".")[0]
    year, month, day = datetime[:4], datetime[4:6], datetime[6:8]
    time = datetime[8:14]
    target_path = f"{prefix}/{cluster_id[:2]}/{cluster_id}/{year}{month}/{day}/{time}.tar.gz"
    return target_path


def create_metadata(s3_path, cluster_id):
    """Create a metadata for Publishing."""
    org_id = extract_org_id(s3_path)
    msg_metadata = {"cluster_id": cluster_id, "external_organization": org_id}
    LOG.debug("msg_metadata %s", msg_metadata)
    return msg_metadata


class S3UploadEngine(Engine):
    """Engine for processing the metadata and ceph path of downloaded archive and uploading it to ceph bucket."""  # noqa: E501

    def __init__(self, **kwargs):
        """Inicialize engin for s3."""
        formatter = kwargs.get("formatter", None)
        target_components = kwargs.get("target_components", None)
        extract_timeout = kwargs.get("extract_timeout", None)
        extract_tmp_dir = kwargs.get("extract_tmp_dir", None)
        self.dest_bucket = kwargs.get("dest_bucket", None)
        self.access_key = kwargs.get("access_key", None)
        self.secret_key = kwargs.get("secret_key", None)
        self.endpoint = kwargs.get("endpoint")
        self.archives_path_prefix = kwargs.get("archives_path_prefix", None)
        self.uploader = S3Uploader(
            access_key=self.access_key, secret_key=self.secret_key, endpoint=self.endpoint
        )  # noqa: E501
        super().__init__(formatter, target_components, extract_timeout, extract_tmp_dir)

    def process(self, broker, local_path):
        """Create a metadata and target_path from downloaded archive and uploads it to ceph bucket."""  # noqa: E501
        cluster_id = broker["cluster_id"]
        s3_path = broker["s3_path"]
        del broker["cluster_id"]
        del broker["s3_path"]
        for w in self.watchers:
            w.watch_broker(broker)

        target_path = compute_target_path(s3_path, self.archives_path_prefix)
        LOG.info(f"Uploading archive '{s3_path}' as {self.dest_bucket}/{target_path}")
        self.uploader.upload_file(local_path, self.dest_bucket, target_path)
        LOG.info(f"Uploaded archive '{s3_path}' as {self.dest_bucket}/{target_path}")

        metadata = create_metadata(s3_path, cluster_id)
        kafka_msg = {
            "path": target_path,
            "original_path": s3_path,
            "metadata": metadata,
        }

        return json.dumps(kafka_msg)
