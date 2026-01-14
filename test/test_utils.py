# Copyright 2020 Red Hat, Inc
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

"""Module with utilities for the tests."""

import time


def mock_consumer_process_no_action_catch_exception(duration_s=0):
    """Mock the process method of ICM consumer so it does nothing.

    This mocks the process method and removes all processing, but
    still catches the raised exceptions.
    If duration_s is provided, the process method is made to sleep
    for the given amount of seconds before returning, unless something
    raises an exception within that timeframe.
    """
    try:
        time.sleep(duration_s)
    except Exception:
        raise
