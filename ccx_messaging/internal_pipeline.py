"""Utilities related to Ingress message format."""

import json
import logging

import jsonschema

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.schemas import ARCHIVE_SYNCED_SCHEMA, RULES_RESULTS_SCHEMA


LOG = logging.getLogger(__name__)


def parse_msg(message: bytes, schema: dict) -> dict:
    """Parse a bytes messages into a dictionary, decoding encoded values."""
    try:
        deserialized_message = json.loads(message)
        jsonschema.validate(instance=deserialized_message, schema=schema)

    except TypeError as ex:
        LOG.warning("Incorrect message type: %s", message)
        raise CCXMessagingError("Incorrect message type") from ex

    except json.JSONDecodeError as ex:
        LOG.warning("Unable to decode received message: %s", message)
        raise CCXMessagingError("Unable to decode received message") from ex

    except jsonschema.ValidationError as ex:
        LOG.warning("Invalid input message JSON schema: %s", deserialized_message)
        raise CCXMessagingError("Invalid input message JSON schema") from ex

    LOG.debug("JSON schema validated: %s", deserialized_message)
    return deserialized_message


def parse_archive_sync_msg(message: bytes) -> dict:
    """Parse archive sync message into dictionary."""
    return parse_msg(message, ARCHIVE_SYNCED_SCHEMA)


def parse_rules_results_msg(message: bytes) -> dict:
    """Parse rules results message into dictionary."""
    return parse_msg(message, RULES_RESULTS_SCHEMA)
