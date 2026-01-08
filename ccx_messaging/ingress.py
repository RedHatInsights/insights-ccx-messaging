"""Utilities related to Ingress message format."""

import base64
import binascii
import json
import logging

import jsonschema

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.schemas import IDENTITY_SCHEMA, INPUT_MESSAGE_SCHEMA


LOG = logging.getLogger(__name__)


def parse_identity(encoded_identity: bytes) -> dict:
    """Generate a dictionary object from a base64 encoded input."""
    try:
        decoded_identity = base64.b64decode(encoded_identity)
        identity = json.loads(decoded_identity)

        jsonschema.validate(instance=identity, schema=IDENTITY_SCHEMA)
        return identity

    except TypeError as ex:
        LOG.warning("Bad argument type %s", encoded_identity)
        raise CCXMessagingError("Bad argument type") from ex

    except binascii.Error as ex:
        LOG.warning("Base64 encoded identity could not be parsed: %s", encoded_identity)
        raise CCXMessagingError("Base64 encoded identity could not be parsed") from ex

    except json.JSONDecodeError as ex:
        LOG.warning("Unable to decode received message: %s", decoded_identity)
        raise CCXMessagingError("Unable to decode received message") from ex

    except jsonschema.ValidationError as ex:
        LOG.warning("Invalid input message JSON schema: %s", identity)
        raise CCXMessagingError("Invalid input message JSON schema") from ex


def parse_ingress_message(message: bytes) -> dict:
    """Parse a bytes messages into a dictionary, decoding encoded values."""
    try:
        deserialized_message = json.loads(message)
        jsonschema.validate(instance=deserialized_message, schema=INPUT_MESSAGE_SCHEMA)

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

    encoded_identity = deserialized_message.pop("b64_identity")
    identity = parse_identity(encoded_identity)
    deserialized_message["identity"] = identity
    return deserialized_message
