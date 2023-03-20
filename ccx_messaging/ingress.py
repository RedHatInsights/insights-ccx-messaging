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
        raise CCXMessagingError(
            "Bad argument type %s", encoded_identity
        ) from ex

    except binascii.Error as ex:
        raise CCXMessagingError(
            f"Base64 encoded identity could not be parsed: {encoded_identity}"
        ) from ex

    except json.JSONDecodeError as ex:
        raise CCXMessagingError(f"Unable to decode received message: {decoded_identity}") from ex

    except jsonschema.ValidationError as ex:
        raise CCXMessagingError(f"Invalid input message JSON schema: {identity}") from ex


def parse_ingress_message(message: bytes) -> dict:
    """Parse a bytes messages into a dictionary, decoding encoded values."""
    try:
        deserialized_message = json.loads(message)
        jsonschema.validate(instance=deserialized_message, schema=INPUT_MESSAGE_SCHEMA)

    except TypeError as ex:
        raise CCXMessagingError(f"Incorrect message type: {message}") from ex

    except json.JSONDecodeError as ex:
        raise CCXMessagingError(f"Unable to decode received message: {message}") from ex

    except jsonschema.ValidationError as ex:
        raise CCXMessagingError(
            f"Invalid input message JSON schema: {deserialized_message}"
        ) from ex

    LOG.debug("JSON schema validated: %s", deserialized_message)

    encoded_identity = deserialized_message.pop("b64_identity")
    identity = parse_identity(encoded_identity)
    deserialized_message["identity"] = identity
    return deserialized_message
