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

"""Module containing unit tests for the `Consumer` class."""

import sys
import pytest


import ccx_messaging.command_line as command_line


def test_command_line_args_valid_flag_version():
    """Verify correct parsing of --version flag."""
    sys.argv = ["ccx-messaging", "--version"]
    parser = command_line.parse_args()
    assert not parser.config
    assert parser.version


def test_command_line_args_valid_config_value():
    """Verify correct parsing of positional argument <config>."""
    sys.argv = ["ccx-messaging", "path_to_config_file"]
    parser = command_line.parse_args()
    assert not parser.version
    assert parser.config


def test_command_line_args_no_config_provided():
    """Verify app does not start if no config provided and exit with code 1."""
    with pytest.raises(SystemExit) as exception:
        sys.argv = ["ccx-messaging"]
        command_line.ccx_messaging()

    assert exception.type is SystemExit
    assert exception.value.code == 1


def test_command_line_args_invalid_arg_provided(capsys):
    """Verify help is shown if invalid argument provided."""
    sys.argv = ["ccx-messaging", "--config"]
    with pytest.raises(SystemExit) as exception:
        parser = command_line.parse_args()

        assert exception.type is SystemExit
        assert exception.value.code == 2

        assert not parser.config
        assert not parser.version

        captured = capsys.readouterr()
        assert "usage: ccx-messaging" in captured.out
        assert "error: unrecognized arguments" in captured.out
