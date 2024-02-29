import json
import os
import re
import logging
from insights_messaging.engine import Engine as ICMEngine
from ccx_messaging.utils.idp_uploader import IDPUploader
from ccx_messaging.error import CCXMessagingError

S3_ARCHIVE_PATTERN = re.compile(r"^([0-9]+)\/([0-9,a-z,-]{36})\/([0-9]{14})-[a-z,A-Z,0-9]*$")
ARCHIVES_PATH_PREFIX = os.environ.get("SYNCED_ARCHIVES_PATH_PREFIX", "archives/compressed")
LOG = logging.getLogger(__name__)

def extract_org_id(file_path):
    search = re.search(S3_ARCHIVE_PATTERN,file_path)
    if not search:
        LOG.warning(
            "Unrecognized archive path. Can't search any '%s' pattern for '%s' path.",
            S3_ARCHIVE_PATTERN,
            file_path,  # noqa 501
        )
        return
    return search.group(1)

def compute_target_path(file_path):
    """
    Computes S3 target path from the source path.
    Target path is in archives/compressed/$ORG_ID/$CLUSTER_ID/$YEAR$MONTH/$DAY/$TIME.tar.gz format.
    """  # noqa: D205, D212, D401
    search = search = re.search(S3_ARCHIVE_PATTERN, file_path)
    if not search:
        LOG.warning(
            "Unrecognized archive path. Can't search any '%s' pattern for '%s' path.",
            S3_ARCHIVE_PATTERN,
            file_path,  # noqa 501
        )
        raise CCXMessagingError("Unable to compute target path")

    cluster_id = f"{search.group(2)}"
    archive = search.group(3)
    datetime = archive.split(".")[0]
    year, month, day = datetime[:4], datetime[4:6], datetime[6:8]
    time = datetime[8:14]
    target_path = (
        f"{ARCHIVES_PATH_PREFIX}"
        + f"/{cluster_id[:2]}"
        + f"/{cluster_id}"
        + f"/{year}{month}"
        + f"/{day}"
        + f"/{time}.tar.gz"
    )
    return target_path

def create_metadata(s3_path,cluster_id):
    org_id = extract_org_id(s3_path)
    msg_metadata = {"cluster_id": cluster_id, "external_organization":org_id}
    LOG.debug("msg_metadata %s", msg_metadata)
    return msg_metadata

class IDPEngine(ICMEngine):

    def __init__(self, formatter,uploader, target_components=None, extract_timeout=None, extract_tmp_dir=None):  # noqa: E501
        self.dest_bucket = uploader.pop("bucket")
        self.uploader = IDPUploader(**uploader)
        super().__init__(formatter, target_components, extract_timeout, extract_tmp_dir)
        
    def process(self, broker, local_path):
        self.dest_bucket = "PlaceholderText"
        cluster_id = broker["cluster_id"]
        s3_path = broker["s3_path"]
        del broker["cluster_id"]
        del broker["s3_path"]
        for w in self.watchers:
            w.watch_broker(broker)
        target_path = compute_target_path(s3_path)
        LOG.info(f"(NOT IMPLEMENTED) Uploading archive '{s3_path}' as {self.dest_bucket}/{target_path}")  # noqa: E501
        self.uploader.upload_file(local_path, self.dest_bucket, target_path)
        LOG.info(f"Uploaded archive '{s3_path}' as {self.dest_bucket}/{target_path}")
        metadata = create_metadata(s3_path,cluster_id)
        kafka_msg = {"path":target_path,"original_path":s3_path,"metadata":metadata}
        return json.dumps(kafka_msg)
