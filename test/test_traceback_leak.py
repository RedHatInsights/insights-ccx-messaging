"""Tests for memory leak fix in SentryMonitoredBroker traceback handling."""

import gc
import traceback
import weakref
from unittest.mock import patch

from insights.core.dr import MissingRequirements
from insights.core.spec_factory import ContentException

from ccx_messaging.monitored_broker import SentryMonitoredBroker


def _make_exception_with_traceback(exc_type=Exception, msg="test exception"):
    """Raise and catch an exception so it has a real __traceback__."""
    try:
        raise exc_type(msg)
    except exc_type as ex:
        tb_string = traceback.format_exc()
        return ex, tb_string


def test_traceback_cleared_generic_exception():
    """Verify __traceback__ is cleared for generic exceptions."""
    broker = SentryMonitoredBroker()
    ex, tb = _make_exception_with_traceback(Exception, "generic error")
    assert ex.__traceback__ is not None

    with patch("ccx_messaging.monitored_broker.capture_exception"):
        broker.add_exception("component", ex, tb)

    assert ex.__traceback__ is None


def test_traceback_cleared_keyboard_interrupt():
    """Verify __traceback__ is cleared for BaseException subclasses."""
    broker = SentryMonitoredBroker()
    ex, tb = _make_exception_with_traceback(KeyboardInterrupt, "interrupt")
    assert ex.__traceback__ is not None

    with patch("ccx_messaging.monitored_broker.capture_exception"):
        broker.add_exception("component", ex, tb)

    assert ex.__traceback__ is None


def test_traceback_cleared_missing_requirements():
    """MissingRequirements should also have traceback cleared."""
    broker = SentryMonitoredBroker()
    ex = MissingRequirements(["req1", "req2"])
    try:
        raise ex
    except MissingRequirements:
        tb = traceback.format_exc()
    assert ex.__traceback__ is not None

    broker.add_exception("component", ex, tb)

    assert ex.__traceback__ is None


def test_traceback_cleared_content_exception():
    """ContentException should also have traceback cleared."""
    broker = SentryMonitoredBroker()
    ex, tb = _make_exception_with_traceback(ContentException, "content error")
    assert ex.__traceback__ is not None

    broker.add_exception("component", ex, tb)

    assert ex.__traceback__ is None


def test_formatted_traceback_preserved():
    """The formatted traceback string must still be in broker.tracebacks."""
    broker = SentryMonitoredBroker()
    ex, tb = _make_exception_with_traceback(Exception, "preserved test")

    with patch("ccx_messaging.monitored_broker.capture_exception"):
        broker.add_exception("component", ex, tb)

    assert broker.tracebacks[ex] == tb
    assert "preserved test" in broker.tracebacks[ex]


def test_sentry_called_with_traceback():
    """capture_exception must be called while __traceback__ is still set."""
    broker = SentryMonitoredBroker()
    ex, tb = _make_exception_with_traceback(Exception, "sentry test")

    traceback_was_set = []

    def mock_capture(captured_ex):
        traceback_was_set.append(captured_ex.__traceback__ is not None)

    with patch(
        "ccx_messaging.monitored_broker.capture_exception",
        side_effect=mock_capture,
    ):
        broker.add_exception("component", ex, tb)

    assert traceback_was_set == [True], (
        "capture_exception must be called before __traceback__ is cleared"
    )
    assert ex.__traceback__ is None


def test_sentry_not_called_for_excluded_types():
    """MissingRequirements and ContentException skip Sentry but still clear traceback."""
    broker = SentryMonitoredBroker()

    for exc_type, msg in [
        (MissingRequirements, ["req"]),
        (ContentException, "content"),
    ]:
        ex = exc_type(msg)
        try:
            raise ex
        except type(ex):
            tb = traceback.format_exc()

        with patch("ccx_messaging.monitored_broker.capture_exception") as mock:
            broker.add_exception("component", ex, tb)
            assert not mock.called
            assert ex.__traceback__ is None


def test_broker_collected_after_add_exception():
    """Without the fix, circular references keep brokers alive."""
    n_brokers = 5
    refs = []

    gc.collect()
    was_enabled = gc.isenabled()
    gc.disable()

    try:
        for _ in range(n_brokers):
            broker = SentryMonitoredBroker()
            ex, tb = _make_exception_with_traceback(Exception, "gc test")

            with patch("ccx_messaging.monitored_broker.capture_exception"):
                broker.add_exception("component", ex, tb)

            refs.append(weakref.ref(broker))
            del broker, ex, tb

        collected = sum(1 for ref in refs if ref() is None)
        assert collected >= n_brokers - 1, (
            f"Only {collected}/{n_brokers} brokers were collected "
            "by reference counting alone. Remaining brokers are held "
            "by circular references from ex.__traceback__ -> frame -> broker."
        )
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()
