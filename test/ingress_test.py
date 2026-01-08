"""Contain unit tests for the ingress module."""

import pytest

from ccx_messaging.error import CCXMessagingError
from ccx_messaging.ingress import parse_identity, parse_ingress_message


VALID_ENCODED_IDENTITIES = [
    "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICIxMjM0NTY3OCJ9fX0K",
]


@pytest.mark.parametrize("valid_encoded_identity", VALID_ENCODED_IDENTITIES)
def test_parse_indentity_valid_values(valid_encoded_identity):
    """Check the behaviour with valid parameters of parse_identity."""
    identity = parse_identity(valid_encoded_identity)
    assert "identity" in identity
    assert "internal" in identity["identity"]
    assert "org_id" in identity["identity"]["internal"]


INVALID_IDENTITY_VALUES = [
    None,
    "asdasd",
    b"{}",
    b"SGVsbG8gd29ybGQ=",
    b"e30=",
]


@pytest.mark.parametrize("invalid_value", INVALID_IDENTITY_VALUES)
def test_parse_indentity_invalid_values(invalid_value):
    """Check that parse_identity catches all the exceptions and raises the expected one."""
    with pytest.raises(CCXMessagingError):
        parse_identity(invalid_value)


VALID_VALUES = [
    '{"url": "","b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICIxMjM0NTY3OCJ9fX0K",'  # noqa E501
    '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
    '{"url": "https://s3.com/hash", "unused-property": null, '
    '"b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICIxMjM0NTY3OCJ9fX0K",'
    '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
    '{"account":12345678, "url":"any/url", '
    '"b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICIxMjM0NTY3OCJ9fX0K",'
    '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
]


@pytest.mark.parametrize("valid_value", VALID_VALUES)
def test_parse_ingress_message_valid(valid_value):
    """Check when parse_identity works with valid values."""
    parsed_msg = parse_ingress_message(valid_value)

    assert "url" in parsed_msg
    assert "identity" in parsed_msg
    assert "timestamp" in parsed_msg


INVALID_VALUES = [
    None,
    1,
    10.0,
    "",
    "{}",
    '{"url":"any/url"}',
]


@pytest.mark.parametrize("invalid_value", INVALID_VALUES)
def test_parse_ingress_message_invalid(invalid_value):
    """Check when parse_identity works with valid values."""
    with pytest.raises(CCXMessagingError):
        parse_ingress_message(invalid_value)
