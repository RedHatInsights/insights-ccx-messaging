"""Utility broker to improve Ssentry exception handling."""

import logging

from insights.core.dr import Broker, MissingRequirements
from insights.core.spec_factory import ContentException
from sentry_sdk import capture_exception


class SentryMonitoredBroker(Broker):
    """Implementation of Broker with custom Sentry capturing logic."""

    def __init__(self, *args, **kwargs):
        """Initialize the SentryMonitoredBroker."""
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)

    def add_exception(self, component, ex, tb=None):
        """Check added exception in order to use it with Sentry or not."""
        super().add_exception(component, ex, tb)

        # prevent MissingRequirements and ContentException from being sent to sentry
        if not isinstance(ex, (MissingRequirements, ContentException)):
            self.logger.debug("Sending exception to Sentry: %s", type(ex))
            # Sentry failure must not prevent traceback cleanup below;
            # otherwise the memory leak silently returns in production.
            try:
                capture_exception(ex)
            except Exception:
                self.logger.exception("Failed to send exception to Sentry")

        # Break circular reference chain to prevent memory leak.
        # TODO: remove when this is merged: https://github.com/RedHatInsights/insights-core/pull/4763
        # Python attaches a live traceback to ex.__traceback__ which
        # references the stack frame (including the broker), creating:
        #   broker.exceptions -> ex -> __traceback__ -> frame -> broker
        # The formatted traceback string is already stored in
        # broker.tracebacks via super().add_exception(), and
        # capture_exception() above receives the live traceback for
        # error-tracking systems (Sentry/GlitchTip), so no debugging
        # information is lost.
        if isinstance(ex, BaseException):
            ex.__traceback__ = None
            # Chained exceptions (`raise X from Y` or implicit chaining)
            # carry their own __traceback__ objects, creating additional
            # circular reference paths through __cause__ and __context__.
            if isinstance(getattr(ex, "__cause__", None), BaseException):
                ex.__cause__.__traceback__ = None
            if isinstance(getattr(ex, "__context__", None), BaseException):
                ex.__context__.__traceback__ = None
