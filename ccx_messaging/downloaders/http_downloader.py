# Copyright 2022 Red Hat Inc.
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

"""Module that defines a Downloader object to get HTTP urls."""

import logging
import re
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import requests

from ccx_messaging.error import CCXMessagingError

LOG = logging.getLogger(__name__)


def parse_human_input(file_size):
    """Parse an input in human-readable format and return a number of bytes."""
    multipliers = {
        "K": 10**3,
        "M": 10**6,
        "G": 10**9,
        "T": 10**12,
        "Ki": 2**10,
        "Mi": 2**20,
        "Gi": 2**30,
        "Ti": 2**40,
    }

    match = re.match(r"^(?P<quantity>\d+(\.\d+)?)\s*(?P<units>[KMGT]?i?B?)?$", file_size)

    if match is None:
        raise ValueError(f"The file size cannot be parsed as a file size: {file_size}")

    parsed = match.groupdict()
    quantity = float(parsed.get("quantity"))

    units = parsed.get("units")
    units = units.rstrip("B") if units is not None else ""

    if units != "" and units not in multipliers:
        raise ValueError(f"The file size cannot be parsed because its units: {parsed.get('units')}")

    multiplier = multipliers.get(units, 1)  # if multiplier == "", then 1
    quantity = quantity * multiplier
    return int(quantity)


# pylint: disable=too-few-public-methods
class HTTPDownloader:
    """Downloader for HTTP uris."""

    # https://<hostname>/service_id/file_id?<credentials and other params>
    HTTP_RE = re.compile(
        r"^(?:https://[^/]+\.s3\.amazonaws\.com/[0-9a-zA-Z/\-]+|"
        r"https://s3\.[0-9a-zA-Z\-]+\.amazonaws\.com/[0-9a-zA-Z\-]+/[0-9a-zA-Z/\-]+|"
        r"http://minio:9000/insights-upload-perma/[0-9a-zA-Z\.\-]+/[0-9a-zA-Z\-]+)\?"
        r"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=[^/]+$"
    )

    def __init__(self, max_archive_size=None, allow_unsafe_links=False):
        """`HTTPDownloader` initializer.

        This method accepts a `max_archive_size` argument, that indicates the
        maximum size allowed for the archives. If set, archives bigger than this
        will be discarded.
        """
        if max_archive_size is not None:
            self.max_archive_size = parse_human_input(max_archive_size)
            LOG.info("Configured max_archive_size to %s bytes", self.max_archive_size)

        else:
            self.max_archive_size = None
            LOG.warning("No max_archive_size defined. Be careful")

        self.allow_unsafe_links = allow_unsafe_links

    @contextmanager
    def get(self, src):
        """Download a file from HTTP server and store it in a temporary file."""
        if not self.allow_unsafe_links:
            if src is None or not HTTPDownloader.HTTP_RE.fullmatch(src):
                LOG.warning("Invalid URL format: %s", src)
                raise CCXMessagingError("Invalid URL format")

        try:
            response = requests.get(src)
            data = response.content
            size = len(data)

            if size == 0:
                LOG.warning("Empty input archive from: %s", src)
                raise CCXMessagingError("Empty input archive")

            if self.max_archive_size is not None and size > self.max_archive_size:
                LOG.warning("The archive is too big ({size} > {self.max_archive_size})", size=size)
                raise CCXMessagingError("The archive is too big. Skipping")

            with NamedTemporaryFile() as file_data:
                file_data.write(data)
                file_data.flush()
                yield file_data.name
            response.close()

        except requests.exceptions.ConnectionError as err:
            LOG.error("Connection error while downloading the file: %s", err)
            raise CCXMessagingError("Connection error while downloading the file") from err
        except CCXMessagingError as err:
            additional_data = getattr(err, "additional_data", None)
            if additional_data is not None:
                LOG.error(
                    "Unknown error while downloading the file: %s",
                    err,
                    extra=additional_data,
                )
            else:
                LOG.error("Unknown error while downloading the file: %s", err)
            raise CCXMessagingError("Unknown error while downloading the file") from err
        except Exception as err:
            LOG.error("Unknown error while downloading the file: %s", err)
            raise CCXMessagingError("Unknown error while downloading the file") from err
