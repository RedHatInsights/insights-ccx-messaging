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


def test_traceback_cleared_keyboard_interrupt():
    """Verify __traceback__ is cleared for BaseException subclasses."""
    broker = SentryMonitoredBroker()
    ex, tb = _make_exception_with_traceback(KeyboardInterrupt, "interrupt")
    assert ex.__traceback__ is not None

    with patch("ccx_messaging.monitored_broker.capture_exception"):
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


def test_traceback_cleared_even_when_sentry_fails():
    """Traceback must be cleared even if capture_exception raises."""
    broker = SentryMonitoredBroker()
    ex, tb = _make_exception_with_traceback(Exception, "sentry failure")

    with patch(
        "ccx_messaging.monitored_broker.capture_exception",
        side_effect=RuntimeError("sentry unavailable"),
    ):
        broker.add_exception("component", ex, tb)

    assert ex.__traceback__ is None


def test_chained_exception_tracebacks():
    """Chained exception tracebacks (__cause__, __context__) should also be cleared."""
    broker = SentryMonitoredBroker()
    caught = None
    try:
        try:
            raise ValueError("original")
        except ValueError as orig:
            raise RuntimeError("wrapper") from orig
    except RuntimeError as ex:
        tb = traceback.format_exc()
        caught = ex

    assert caught.__traceback__ is not None
    assert caught.__cause__.__traceback__ is not None

    with patch("ccx_messaging.monitored_broker.capture_exception"):
        broker.add_exception("component", caught, tb)

    assert caught.__traceback__ is None
    assert caught.__cause__.__traceback__ is None


def test_implicit_chained_exception_context():
    """Implicit chaining (__context__) should also have traceback cleared."""
    broker = SentryMonitoredBroker()
    caught = None
    try:
        try:
            raise ValueError("original")
        except ValueError:
            raise RuntimeError("during handling")  # noqa: B904
    except RuntimeError as ex:
        tb = traceback.format_exc()
        caught = ex

    assert caught.__traceback__ is not None
    assert caught.__context__.__traceback__ is not None

    with patch("ccx_messaging.monitored_broker.capture_exception"):
        broker.add_exception("component", caught, tb)

    assert caught.__traceback__ is None
    assert caught.__context__.__traceback__ is None


def test_multiple_exceptions_all_tracebacks_cleared():
    """All tracebacks must be cleared when multiple exceptions are added to one broker."""
    broker = SentryMonitoredBroker()
    exceptions = []

    for i in range(5):
        ex, tb = _make_exception_with_traceback(Exception, f"error {i}")
        with patch("ccx_messaging.monitored_broker.capture_exception"):
            broker.add_exception(f"component_{i}", ex, tb)
        exceptions.append(ex)

    for ex in exceptions:
        assert ex.__traceback__ is None, f"Traceback not cleared for: {ex}"


def test_exception_without_traceback():
    """Exceptions that were never raised (no __traceback__) should not crash."""
    broker = SentryMonitoredBroker()
    ex = Exception("never raised")
    assert ex.__traceback__ is None

    with patch("ccx_messaging.monitored_broker.capture_exception"):
        broker.add_exception("component", ex, None)

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


def test_broker_collected_with_chained_exceptions():
    """Brokers must be collectable even when exceptions have __cause__ chains."""
    n_brokers = 5
    refs = []

    gc.collect()
    was_enabled = gc.isenabled()
    gc.disable()

    try:
        for _ in range(n_brokers):
            broker = SentryMonitoredBroker()
            caught = None
            try:
                try:
                    raise ValueError("cause")
                except ValueError as orig:
                    raise RuntimeError("effect") from orig
            except RuntimeError as ex:
                tb = traceback.format_exc()
                caught = ex

            with patch("ccx_messaging.monitored_broker.capture_exception"):
                broker.add_exception("component", caught, tb)

            refs.append(weakref.ref(broker))
            del broker, caught, tb

        collected = sum(1 for ref in refs if ref() is None)
        assert collected >= n_brokers - 1, (
            f"Only {collected}/{n_brokers} brokers collected with chained exceptions"
        )
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()
