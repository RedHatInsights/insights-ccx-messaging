# Copyright 2022 Red Hat Inc.
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

"""Module containing the implementation of the `CCXMessagingError` exception class."""


class CCXMessagingError(Exception):
    """Represents a CCX messaging exception.

    This should make it easier to differentiate between
    exceptions caused by internal and external code.
    """

    def __init__(self, message, *args, additional_data=None):
        """Initialize CCXMessagingError with optional additional data.

        Args:
            message: The error message (may contain format specifiers)
            *args: Arguments for string formatting (for backward compatibility)
            additional_data: Optional dict containing additional context data

        Raises:
            TypeError: If additional_data is not None or dict

        """
        super().__init__(message, *args)

        if additional_data is not None and not isinstance(additional_data, dict):
            raise TypeError("additional_data must be a dict or None")
        self.additional_data = additional_data

    def format(self, input_msg):
        """Format the error by adding information about input Kafka message."""
        return f"Status: Error; Topic: {input_msg['topic']}; Cause: {self}"
