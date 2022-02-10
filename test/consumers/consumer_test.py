# Copyright 2021, 2022 Red Hat, Inc
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

import logging
import io
import time

from unittest.mock import patch

import pytest

from kafka import KafkaConsumer, KafkaProducer

from ccx_messaging.consumers.consumer import Consumer
from ccx_messaging.error import CCXMessagingError

from test.utils import mock_consumer_record, mock_consumer_process_no_action_catch_exception


@pytest.fixture(autouse=True)
def mock_consumer(monkeypatch):
    """Mock KafkaConsumer."""
    monkeypatch.setattr(KafkaConsumer, "__init__", lambda *args, **kargs: None)


@pytest.fixture(autouse=True)
def mock_producer(monkeypatch):
    """Mock KafkaProducer."""
    monkeypatch.setattr(KafkaProducer, "__init__", lambda *args, **kargs: None)


@pytest.fixture(autouse=True)
def mock_producer_send(monkeypatch):
    """Mock KafkaProducer send method."""
    monkeypatch.setattr(KafkaProducer, "send", lambda *args, **kargs: None)


_REGEX_BAD_SCHEMA = r"^Unable to extract URL from input message: "
_INVALID_TYPE_VALUES = [None, 42, 3.14, True, [], {}]


@pytest.mark.parametrize("value", _INVALID_TYPE_VALUES)
def test_deserialize_invalid_type(value):
    """Test that passing invalid data type to `deserialize` raises an exception."""
    deserialized = Consumer.deserialize(Consumer(None, None, None), value)
    assert isinstance(deserialized, CCXMessagingError)
    assert str(deserialized).startswith("Unexpected input message type: ")


_ERR_UNABLE_TO_DECODE = "Unable to decode received message: "
_ERR_JSON_SCHEMA = "Invalid input message JSON schema: "
_ERR_BASE64 = "Base64 encoded identity could not be parsed: "

_INVALID_MESSAGES = [
    ("", _ERR_UNABLE_TO_DECODE),
    ("{}", _ERR_JSON_SCHEMA),
    ('{"noturl":"https://s3.com/hash"}', _ERR_JSON_SCHEMA),
    ('{"url":"value"', _ERR_UNABLE_TO_DECODE),
    ('"url":"value"}', _ERR_UNABLE_TO_DECODE),
    ('"url":"value"', _ERR_UNABLE_TO_DECODE),
    ('"{"url":"value"}"', _ERR_UNABLE_TO_DECODE),
    # incorrect content of b64_identity (org_id missing)
    (
        '{"url": "https://s3.com/hash", "b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im1pc'
        '3Npbmdfb3JnX2lkIjogIjEyMzQ1Njc4In19fQo=", "timestamp": "2020-01-23T16:15:59.478901889Z"}',
        _ERR_JSON_SCHEMA,
    ),
    # incorrect format of base64 encoding
    (
        '{"url": "https://s3.com/hash", "b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ1'
        '9pZCI6ICIxMjM0NTY3OCJ9Cg=", "timestamp": "2020-01-23T16:15:59.478901889Z"}',
        _ERR_BASE64,
    ),
    # org_id not string
    (
        '{"url": "https://s3.com/hash", "b64_identity": "eyJpZGVudGl0eSI6IHsKICAgICJhY2NvdW50X251bW'
        'JlciI6ICI5ODc2NTQzIiwKICAgICJhdXRoX3R5cGUiOiAiYmFzaWMtYXV0aCIsCiAgICAiaW50ZXJuYWwiOiB7CiAg'
        'ICAgICAgImF1dGhfdGltZSI6IDE0MDAsCiAgICAgICAgIm9yZ19pZCI6IDEyMzQ1Njc4CiAgICB9LAogICAgInR5cG'
        'UiOiAiVXNlciIsCiAgICAidXNlciI6IHsKICAgICAgICAiZW1haWwiOiAiam9obi5kb2VAcmVkaGF0LmNvbSIsCiAg'
        'ICAgICAgImZpcnN0X25hbWUiOiAiSW5zaWdodHMiLAogICAgICAgICJpc19hY3RpdmUiOiB0cnVlLAogICAgICAgIC'
        'Jpc19pbnRlcm5hbCI6IGZhbHNlLAogICAgICAgICJpc19vcmdfYWRtaW4iOiB0cnVlLAogICAgICAgICJsYXN0X25h'
        'bWUiOiAiUUUiLAogICAgICAgICJsb2NhbGUiOiAiZW5fVVMiLAogICAgICAgICJ1c2VybmFtZSI6ICJpbnNpZ2h0cy'
        '1tYXN0ZXIiCiAgICB9Cn0KfQo=", "timestamp": "2020-01-23T16:15:59.478901889Z"}',
        _ERR_JSON_SCHEMA,
    ),
]


@pytest.mark.parametrize("msg", _INVALID_MESSAGES)
def test_deserialize_invalid_format_str(msg):
    """Test that passing a malformed message to `deserialize` raises an exception."""
    deserialized = Consumer.deserialize(Consumer(None, None, None), msg[0])
    assert isinstance(deserialized, CCXMessagingError)
    assert str(deserialized).startswith(msg[1])


@pytest.mark.parametrize("msg", _INVALID_MESSAGES)
def test_deserialize_invalid_format_bytes(msg):
    """Test that passing a malformed message to `deserialize` raises an exception."""
    deserialized = Consumer.deserialize(Consumer(None, None, None), msg[0].encode("utf-8"))
    assert isinstance(deserialized, CCXMessagingError)
    assert str(deserialized).startswith(msg[1])


@pytest.mark.parametrize("msg", _INVALID_MESSAGES)
def test_deserialize_invalid_format_bytearray(msg):
    """Test that passing a malformed message to `deserialize` raises an exception."""
    deserialized = Consumer.deserialize(
        Consumer(None, None, None), bytearray(msg[0].encode("utf-8"))
    )
    assert isinstance(deserialized, CCXMessagingError)
    assert str(deserialized).startswith(msg[1])


_VALID_MESSAGES = [
    (
        '{"url": "",'
        '"b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICIxMjM0NTY3OCJ9fX0K",'
        '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
        {
            "url": "",
            "identity": {"identity": {"internal": {"org_id": "12345678"}}},
            "timestamp": "2020-01-23T16:15:59.478901889Z",
            "ClusterName": None,
        },
    ),
    (
        '{"url": "https://s3.com/hash", "unused-property": null, '
        '"b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICIxMjM0NTY3OCJ9fX0K",'
        '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
        {
            "url": "https://s3.com/hash",
            "unused-property": None,
            "identity": {"identity": {"internal": {"org_id": "12345678"}}},
            "timestamp": "2020-01-23T16:15:59.478901889Z",
            "ClusterName": None,
        },
    ),
    (
        '{"account":12345678, "url":"any/url", '
        '"b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICIxMjM0NTY3OCJ9fX0K",'
        '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
        {
            "account": 12345678,
            "url": "any/url",
            "identity": {"identity": {"internal": {"org_id": "12345678"}}},
            "timestamp": "2020-01-23T16:15:59.478901889Z",
            "ClusterName": None,
        },
    ),
]


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
def test_deserialize_valid_str(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    assert Consumer.deserialize(Consumer(None, None, None), msg) == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
def test_deserialize_valid_bytes(msg, value):
    """Test that proper bytes JSON input messages are correctly deserialized."""
    retval = Consumer.deserialize(Consumer(None, None, None), msg.encode("utf-8"))
    assert retval == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
def test_deserialize_valid_bytearray(msg, value):
    """Test that proper bytearray JSON input messages are correctly deserialized."""
    retval = Consumer.deserialize(Consumer(None, None, None), bytearray(msg.encode("utf-8")))
    assert retval == value


# This would have been a valid input, but it's supposed to be a `dict`, not `str`.
_DICT_STR = '{"url": "bucket/file"}'

_INVALID_RECORD_VALUES = [
    "",
    _DICT_STR,
    _DICT_STR.encode("utf-8"),
    bytearray(_DICT_STR.encode("utf-8")),
    [],
    {},
    {"noturl": "bucket/file"},
]


@pytest.mark.parametrize("value", _INVALID_RECORD_VALUES)
@patch(
    "ccx_messaging.consumers.consumer.KafkaConsumer.__init__",
    lambda *args, **kwargs: None,
)
def test_handles_invalid(value):
    """Test that `Consumer` refuses to handle malformed input messages."""
    consumer = Consumer(None, None, None)
    assert not consumer.handles(mock_consumer_record(value))


_VALID_RECORD_VALUES = [
    {"url": ""},
    {"url": "bucket/file"},
    {"url": "https://a-valid-domain.com/precious_url"},
]


@pytest.mark.parametrize("value", _VALID_RECORD_VALUES)
@patch("insights_messaging.consumers.Consumer.__init__", lambda *a, **k: None)
def test_handles_valid(value):
    """Test that `Consumer` accepts handling of correctly formatted input messages."""
    sut = Consumer(None, None, None)
    assert sut.handles(mock_consumer_record(value))


@pytest.mark.parametrize("value", _INVALID_RECORD_VALUES)
def test_get_url_invalid(value):
    """Test that `Consumer.get_url` raises the appropriate exception."""
    with pytest.raises(CCXMessagingError, match=_REGEX_BAD_SCHEMA):
        Consumer.get_url(None, value)


@pytest.mark.parametrize("value", _VALID_RECORD_VALUES)
def test_get_url_valid(value):
    """Test that `Consumer.get_url` returns the expected value."""
    assert Consumer.get_url(None, mock_consumer_record(value)) == value["url"]


_VALID_TOPICS = ["topic", "funny-topic"]

_VALID_GROUPS = ["group", "good-boys"]

_VALID_SERVERS = ["server", "great.server.net"]


@pytest.mark.parametrize("topic", _VALID_TOPICS)
@pytest.mark.parametrize("group", _VALID_GROUPS)
@pytest.mark.parametrize("server", _VALID_SERVERS)
def test_consumer_init_direct(topic, group, server):
    """Test of our Consumer constructor, using direct configuration options."""
    with patch("insights_messaging.consumers.Consumer.__init__") as mock_consumer_init:
        with patch("os.environ", new=dict()):
            Consumer(None, None, None, group, topic, [server])

            mock_consumer_init.assert_called_with(None, None, None)


MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST = 2


@patch("insights_messaging.consumers.Consumer.__init__", lambda *a, **k: None)
@patch(
    "ccx_messaging.consumers.consumer.MAX_ELAPSED_TIME_BETWEEN_MESSAGES",
    MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST,
)
def test_elapsed_time_thread_no_warning_when_message_received():
    """
    Test elapsed time thread if new message received on time.

    Test that no warnings are sent if a new message is received before
    the defined MAX_ELAPSED_TIME_BETWEEN_MESSAGES.
    """
    buf = io.StringIO()
    log_handler = logging.StreamHandler(buf)

    logger = logging.getLogger()
    logger.level = logging.DEBUG
    logger.addHandler(log_handler)

    with patch("ccx_messaging.consumers.consumer.LOG", logger):
        sut = Consumer(None, None, None)
        assert sut.check_elapsed_time_thread
        buf.truncate(0)  # Empty buffer to make sure this test does what it should do
        sut.last_received_message_time = time.time()
        assert "No new messages in the queue since " not in buf.getvalue()
        time.sleep(MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST - 1)
        sut.last_received_message_time = time.time()
        assert "No new messages in the queue since " not in buf.getvalue()

    logger.removeHandler(log_handler)


@patch("insights_messaging.consumers.Consumer.__init__", lambda *a, **k: None)
@patch(
    "ccx_messaging.consumers.consumer.MAX_ELAPSED_TIME_BETWEEN_MESSAGES",
    MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST,
)
def test_elapsed_time_thread_warning_when_no_message_received():
    """
    Test elapsed time thread if no new message received on time.

    Test that warnings are sent if no new messages are received before
    the defined MAX_ELAPSED_TIME_BETWEEN_MESSAGES.
    """
    buf = io.StringIO()
    log_handler = logging.StreamHandler(buf)

    logger = logging.getLogger()
    logger.level = logging.DEBUG
    logger.addHandler(log_handler)

    with patch("ccx_messaging.consumers.consumer.LOG", logger):
        sut = Consumer(None, None, None, "group", "topic", ["server"])
        assert sut.check_elapsed_time_thread
        alert_time = time.strftime(
            "%Y-%m-%d- %H:%M:%S", time.gmtime(sut.last_received_message_time)
        )
        alert_message = "No new messages in the queue since " + alert_time
        # Make sure the thread woke up at least once
        time.sleep(2 * MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST)
        assert alert_message in buf.getvalue()

    logger.removeHandler(log_handler)


@patch("ccx_messaging.consumers.consumer.Consumer.handles", lambda *a, **k: True)
@patch("ccx_messaging.consumers.consumer.Consumer.fire", lambda *a, **k: None)
@patch("ccx_messaging.consumers.consumer.Consumer.get_stringfied_record", lambda *a, **k: None)
def test_process_message_timeout_no_kafka_requeuer():
    """Test timeout mechanism that wraps the process function."""
    process_message_timeout = 2
    process_message_timeout_elapsed = 3
    process_message_timeout_not_elapsed = 1
    consumer_messages_to_process = _VALID_MESSAGES[0]
    expected_alert_message = "Couldn't process message in the given time frame."

    buf = io.StringIO()
    log_handler = logging.StreamHandler(buf)

    logger = logging.getLogger()
    logger.level = logging.DEBUG
    logger.addHandler(log_handler)

    with patch("ccx_messaging.consumers.consumer.LOG", logger):
        sut = Consumer(None, None, None)
        sut.consumer = consumer_messages_to_process
        assert sut.processing_timeout == 0  # Should be 0 if not changed in config file

        with patch(
            "ccx_messaging.consumers.consumer.Consumer.process",
            lambda *a, **k: mock_consumer_process_no_action_catch_exception(0),
        ):
            sut.run()
            assert expected_alert_message not in buf.getvalue()

        sut.processing_timeout = process_message_timeout

        with patch(
            "ccx_messaging.consumers.consumer.Consumer.process",
            lambda *a, **k: mock_consumer_process_no_action_catch_exception(
                process_message_timeout_not_elapsed
            ),
        ):
            sut.run()
            assert expected_alert_message not in buf.getvalue()

        with patch(
            "ccx_messaging.consumers.consumer.Consumer.process",
            lambda *a, **k: mock_consumer_process_no_action_catch_exception(
                process_message_timeout_elapsed
            ),
        ):
            sut.run()
            assert expected_alert_message in buf.getvalue()

    logger.removeHandler(log_handler)
