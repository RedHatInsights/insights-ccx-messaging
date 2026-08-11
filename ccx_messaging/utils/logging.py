# Copyright 2023 Red Hat Inc.
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

"""Submodule to configure logging stuff that cannot be afford from configuration file."""

import logging
import os
import platform
import uuid
from urllib.parse import urlparse

from boto3.session import Session
from pythonjsonlogger import json as jsonlogger
from watchtower import CloudWatchLogHandler


def setup_watchtower(logging_config=None):
    """Setups the CloudWatch handler if the proper configuration is provided."""
    enabled = os.getenv("LOGGING_TO_CW_ENABLED", "False").lower() in ("true", "1", "t", "yes")
    if not enabled:
        return

    aws_config_vars = (
        "CW_AWS_ACCESS_KEY_ID",
        "CW_AWS_SECRET_ACCESS_KEY",
        "AWS_REGION_NAME",
        "CW_LOG_GROUP",
        "CW_STREAM_NAME",
    )

    if any(os.environ.get(key, "").strip() == "" for key in aws_config_vars):
        return

    # Get the log level from CW_LOG_LEVEL. If not, default to INFO
    log_level = logging.getLevelName(os.getenv("CW_LOG_LEVEL", "INFO"))

    session = Session(
        aws_access_key_id=os.environ["CW_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["CW_AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_REGION_NAME"],
    )
    client = session.client("logs")

    root_logger = logging.getLogger()

    handler = CloudWatchLogHandler(
        boto3_client=client,
        log_group_name=os.environ["CW_LOG_GROUP"],
        log_stream_name=os.environ["CW_STREAM_NAME"],
        create_log_group=False,
    )

    if logging_config is not None:
        log_format = logging_config.get("formatters", {}).get("cloudwatch", {}).get("format")
        handler.setFormatter(CloudWatchFormatter(log_format))

    try:
        handler.setLevel(log_level)

    except ValueError:
        root_logger.warning(
            "Log level for cloudwatch cannot be set to %s. Default to INFO", log_level
        )
        handler.setLevel(logging.INFO)

    root_logger.addHandler(handler)


class CloudWatchFormatter(jsonlogger.JsonFormatter):
    """Class that implements formatter for logging to CloudWatch."""

    def __init__(self, *args, **kwargs):
        """Initialize CloudWatchFormatter."""
        super().__init__(*args, **kwargs)

        self.hostname = platform.node()
        self.mac_address = get_mac_address()

    def format(self, record):
        """Format the record."""
        record.mac_address = self.mac_address
        record.hostname = self.hostname
        return super().format(record)


def get_mac_address():
    """Get mac address or None if it is not possible."""
    mac_address = uuid.getnode()

    # Ignore if it wasn't successful, see help of uuid.getnode():

    # If all attempts to obtain the hardware address fail, we
    # choose a random 48-bit number with its eighth bit set to 1 as recommended
    # in RFC 4122.

    # By 8th they meant 40th. From sources of getnode:
    # def _random_getnode():
    #     """Get a random node ID."""
    #     # RFC 4122, $4.1.6 says "For systems with no IEEE address, a randomly or
    #     # pseudo-randomly generated value may be used; see Section 4.5.  The
    #     # multicast bit must be set in such addresses, in order that they will
    #     # never conflict with addresses obtained from network cards."
    #     #
    #     # The "multicast bit" of a MAC address is defined to be "the least
    #     # significant bit of the first octet".  This works out to be the 41st bit
    #     # counting from 1 being the least significant bit, or 1<<40.
    #     #
    #     # See https://en.wikipedia.org/wiki/MAC_address#Unicast_vs._multicast
    #     import random
    #     return random.getrandbits(48) | (1 << 40)
    if (mac_address >> 40) & 1:
        # if this bit is 1, ignore, it's better than have bunch of random numbers in logs
        mac_address = None

    # finally format it to the human readable format
    if mac_address is not None:
        mac_address = ":".join(reversed([hex((mac_address >> i * 8) & 0xFF)[2:] for i in range(6)]))

    return mac_address


def anonymize_url(url: str) -> str:
    """Anonymize an URL, keeping only the protocol and the domain.

    Any authentication information (user/password embedded in the URL) and
    the endpoint (path, query string and fragment, which often contain
    presigned URL signatures or other sensitive data) are replaced by
    "****".

    Args:
        url: The URL to anonymize.

    Returns:
        The anonymized URL, e.g. "https://example.com/****", or "****" if
        `url` is not a string or could not be parsed as a valid URL (this
        includes malformed ports, e.g. non-numeric or out-of-range values).
    """
    if not isinstance(url, str):
        return "****"

    try:
        parsed_url = urlparse(url)

        if not parsed_url.scheme or not parsed_url.hostname:
            return "****"

        domain = parsed_url.hostname
        if parsed_url.port:
            domain = f"{domain}:{parsed_url.port}"

        auth = "****@" if parsed_url.username or parsed_url.password else ""
        endpoint = "/****" if parsed_url.path or parsed_url.query or parsed_url.fragment else ""

    except ValueError:
        return "****"

    return f"{parsed_url.scheme}://{auth}{domain}{endpoint}"


def anonymize_message(value: dict) -> dict:
    """Anonymize the message."""
    if not isinstance(value, dict):
        return value

    new_value = value.copy()

    if "identity" in new_value:
        new_value["identity"] = "anonymized_value"
    if "b64_identity" in new_value:
        new_value["b64_identity"] = "anonymized_b64_identity"
    if "url" in new_value:
        new_value["url"] = anonymize_url(new_value["url"])

    return new_value
