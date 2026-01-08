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

"""Sentry SDK configuration and utility functions."""

import logging
import os

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration


def get_event_level():
    """Get level of events to monitor (errors only, or error and warnings)."""
    if os.environ.get("SENTRY_CATCH_WARNINGS", False):
        return logging.WARNING
    return logging.ERROR


def init_sentry(dsn=None, transport=None, environment=None, enabled=False):
    """Configure and initialize sentry SDK for this project."""
    if enabled:
        if dsn:
            logging.getLogger(__name__).info("Initializing sentry")
            sentry_logging = LoggingIntegration(level=logging.INFO, event_level=get_event_level())

            sentry_sdk.init(
                dsn=dsn,
                ca_certs="/etc/pki/tls/certs/ca-bundle.crt",
                integrations=[sentry_logging],
                max_breadcrumbs=15,
                transport=transport,
                environment=environment,
            )
        else:
            logger = logging.getLogger(__name__)
            logger.warning("Configuration Warning: Sentry is enabled, but no DSN was provided.")
