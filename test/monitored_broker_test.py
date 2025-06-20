"""Tests for the utility class SentryMonitoredBroker."""

import time
from uuid import uuid4
from unittest.mock import patch

import pytest
from insights.core.dr import MissingRequirements
from insights.core.spec_factory import ContentException


from ccx_messaging.monitored_broker import SentryMonitoredBroker


def test_usage():
    """Check that normal usage for the broker is kept across inheritance."""
    broker = SentryMonitoredBroker()
    cluster_id = uuid4()
    timestamp = time.time()

    broker["cluster_id"] = cluster_id
    broker["timestamp"] = timestamp

    assert broker["cluster_id"] == cluster_id
    assert broker["timestamp"] == timestamp


@patch("ccx_messaging.monitored_broker.capture_exception")
def test_add_exception(sentry_capture_exception_mock):
    """Check add_exception behavior."""
    broker = SentryMonitoredBroker()

    broker.add_exception(None, KeyboardInterrupt, None)
    assert sentry_capture_exception_mock.called


NO_CAPTURE_EXCEPTIONS = [MissingRequirements, ContentException]


@pytest.mark.parametrize("exception", NO_CAPTURE_EXCEPTIONS)
def test_add_exception_no_capture(exception):
    """Check that selected exceptions are not captured by Sentry."""
    broker = SentryMonitoredBroker()

    with patch("ccx_messaging.monitored_broker.capture_exception") as capture_exception_mock:
        # Adding an argument to constructor because it is required by some of the exceptions
        broker.add_exception(None, exception("some message"), None)
        assert not capture_exception_mock.called
