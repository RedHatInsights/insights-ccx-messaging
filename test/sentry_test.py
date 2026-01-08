# Copyright 2023 Red Hat, Inc
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

"""Module containing unit tests for the `utils/sentry.py` file."""

import logging
import os

from ccx_messaging.utils.sentry import get_event_level


def test_get_event_level():
    """Verify the `get_event_level` function works.

    Check that it returns `logging.WARNING` if SENTRY_CATCH_WARNINGS
    is set, otherwise return `logging.ERROR`.
    """
    os.environ["SENTRY_CATCH_WARNINGS"] = "1"
    assert get_event_level() == logging.WARNING
    os.environ["SENTRY_CATCH_WARNINGS"] = ""
    assert get_event_level() == logging.ERROR
