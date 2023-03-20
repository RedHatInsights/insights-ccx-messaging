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

"""Module containing unit tests for the `KafkaConsumer` class."""

import logging
import io
import time

from unittest.mock import patch

import pytest

# from kafka import KafkaConsumer, KafkaProducer
# from kafka.consumer.fetcher import ConsumerRecord
from confluent_kafka import Consumer

from ccx_messaging.consumers.kafka_consumer import KafkaConsumer
from ccx_messaging.error import CCXMessagingError

from test.utils import (
    mock_consumer_record,
    mock_consumer_process_no_action_catch_exception,
)


class KafkaMessage:
    """Test double for the confluent_kafka.Message class."""
    def __init__(self, msg, headers=None):
        """Initialize a KafkaMessage test double."""
        self.msg = msg
        self._headers = headers

    def value(self):
        return self.msg
    
    def error(self):
        return None

    def headers(self):
        return self._headers

# _REGEX_BAD_SCHEMA = r"^Unable to extract URL from input message: "
_INVALID_TYPE_VALUES = [
    None,
    42,
    3.14,
    True,
    [],
    {},
]


@pytest.mark.parametrize("value", _INVALID_TYPE_VALUES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_invalid_type(mock_consumer, value):
    """Test that passing invalid data type to `deserialize` raises an exception."""
    sut = KafkaConsumer(None, None, None, None)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(value)


_INVALID_MESSAGES = [
    "",
    "{}",
    '{"noturl":"https://s3.com/hash"}',
    '{"url":"value"',
    '"url":"value"}',
    '"url":"value"',
    '"{"url":"value"}"',
    # incorrect content of b64_identity (org_id missing)
    '{"url": "https://s3.com/hash", "b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im1pc'
    '3Npbmdfb3JnX2lkIjogIjEyMzQ1Njc4In19fQo=", "timestamp": "2020-01-23T16:15:59.478901889Z"}',

    # incorrect format of base64 encoding
    '{"url": "https://s3.com/hash", "b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ1'
    '9pZCI6ICIxMjM0NTY3OCJ9Cg=", "timestamp": "2020-01-23T16:15:59.478901889Z"}',
 
    # org_id not string
    '{"url": "https://s3.com/hash", "b64_identity": "eyJpZGVudGl0eSI6IHsKICAgICJhY2NvdW50X251bW'
    "JlciI6ICI5ODc2NTQzIiwKICAgICJhdXRoX3R5cGUiOiAiYmFzaWMtYXV0aCIsCiAgICAiaW50ZXJuYWwiOiB7CiAg"
    "ICAgICAgImF1dGhfdGltZSI6IDE0MDAsCiAgICAgICAgIm9yZ19pZCI6IDEyMzQ1Njc4CiAgICB9LAogICAgInR5cG"
    "UiOiAiVXNlciIsCiAgICAidXNlciI6IHsKICAgICAgICAiZW1haWwiOiAiam9obi5kb2VAcmVkaGF0LmNvbSIsCiAg"
    "ICAgICAgImZpcnN0X25hbWUiOiAiSW5zaWdodHMiLAogICAgICAgICJpc19hY3RpdmUiOiB0cnVlLAogICAgICAgIC"
    "Jpc19pbnRlcm5hbCI6IGZhbHNlLAogICAgICAgICJpc19vcmdfYWRtaW4iOiB0cnVlLAogICAgICAgICJsYXN0X25h"
    "bWUiOiAiUUUiLAogICAgICAgICJsb2NhbGUiOiAiZW5fVVMiLAogICAgICAgICJ1c2VybmFtZSI6ICJpbnNpZ2h0cy"
    '1tYXN0ZXIiCiAgICB9Cn0KfQo=", "timestamp": "2020-01-23T16:15:59.478901889Z"}',
]


@pytest.mark.parametrize("msg", _INVALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_invalid_format_str(mock_consumer, msg):
    """Test that passing a malformed message to `deserialize` raises an exception."""
    sut = KafkaConsumer(None, None, None, None)
    message = KafkaMessage(msg)
    with pytest.raises(CCXMessagingError):
        sut.deserialize(message)


_VALID_MESSAGES = [
    (
        '{"url": "",'
        '"b64_identity": "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICIxMjM0NTY3OCJ9fX0K",'
        '"timestamp": "2020-01-23T16:15:59.478901889Z"}',
        {
            "url": "",
            "identity": {"identity": {"internal": {"org_id": "12345678"}}},
            "timestamp": "2020-01-23T16:15:59.478901889Z",
            "cluster_name": None,
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
            "cluster_name": None,
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
            "cluster_name": None,
        },
    ),
]


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_valid_str(consumer_mock, msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = KafkaConsumer(None, None, None, None)
    assert sut.deserialize(KafkaMessage(msg)) == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_valid_bytes(consumer_mock, msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = KafkaConsumer(None, None, None, None)
    assert sut.deserialize(KafkaMessage(msg.encode())) == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_deserialize_valid_bytearray(consumer_mock, msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = KafkaConsumer(None, None, None, None)
    assert sut.deserialize(KafkaMessage(bytearray(msg.encode()))) == value


PLATFORM_SERVICE = "some_service"
_NO_HANDLE_HEADERS = [
    None,
    {},
    {"not_expected_key": b"ignored_value"},
    {"service": b"this is not my service"}
]

_HANDLE_HEADER = {"service": b"some_service"}


@pytest.mark.parametrize("headers", _NO_HANDLE_HEADERS)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_platform_filter(consumer_mock, headers):
    """Test that filter by platform behaves as expected."""
    sut = KafkaConsumer(None, None, None, None, platform_service=PLATFORM_SERVICE)
    assert not sut.handles(KafkaMessage("{}", headers))


@pytest.mark.parametrize("headers", _NO_HANDLE_HEADERS + [_HANDLE_HEADER])
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_platform_filter_no_platform_service(consumer_mock, headers):
    """Test that filter by platform behaves as expected."""
    sut = KafkaConsumer(None, None, None, None)
    assert sut.handles(KafkaMessage("{}", headers))


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_platform_filter_platform_service(consumer_mock):
    """Test that filter by platform behaves as expected."""
    sut = KafkaConsumer(None, None, None, None, platform_service=PLATFORM_SERVICE)
    assert sut.handles(KafkaMessage("{}", _HANDLE_HEADER))

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

_VALID_RECORD_VALUES = [
    {"url": ""},
    {"url": "bucket/file"},
    {"url": "https://a-valid-domain.com/precious_url"},
]


@pytest.mark.parametrize("value", _INVALID_RECORD_VALUES)
def test_get_url_invalid(value):
    """Test that `KafkaConsumer.get_url` raises the appropriate exception."""
    with pytest.raises(CCXMessagingError):
        KafkaConsumer.get_url(None, value)


@pytest.mark.parametrize("value", _VALID_RECORD_VALUES)
def test_get_url_valid(value):
    """Test that `Consumer.get_url` returns the expected value."""
    assert KafkaConsumer.get_url(None, value) == value["url"]


_VALID_TOPICS = ["topic", "funny-topic"]
_VALID_GROUPS = ["group", "good-boys"]
_VALID_SERVERS = ["server", "great.server.net"]


@pytest.mark.parametrize("topic", _VALID_TOPICS)
@pytest.mark.parametrize("group", _VALID_GROUPS)
@pytest.mark.parametrize("server", _VALID_SERVERS)
def test_consumer_init_direct(topic, group, server):
    """Test of our Consumer constructor, using direct configuration options."""
    with patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer") as mock_consumer_init:
        with patch("os.environ", new=dict()):
            KafkaConsumer(None, None, None, topic, group_id=group, bootstrap_servers=[server])
            config = {
                "bootstrap.servers": [server],
                "group.id": group,
                "retry.backoff.ms": 1000,
            }
            mock_consumer_init.assert_called_with(config)


MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST = 2


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
@patch(
    "ccx_messaging.consumers.consumer.MAX_ELAPSED_TIME_BETWEEN_MESSAGES",
    MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST,
)
def test_elapsed_time_thread_no_warning_when_message_received(consumer_mock):
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

    with patch("ccx_messaging.consumers.kafka_consumer.LOG", logger):
        sut = KafkaConsumer(None, None, None, None)
        assert sut.check_elapsed_time_thread
        buf.truncate(0)  # Empty buffer to make sure this test does what it should do
        sut.last_received_message_time = time.time()
        assert "No new messages in the queue since " not in buf.getvalue()
        time.sleep(MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST - 1)
        sut.last_received_message_time = time.time()
        assert "No new messages in the queue since " not in buf.getvalue()

    logger.removeHandler(log_handler)


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
@patch(
    "ccx_messaging.consumers.kafka_consumer.MAX_ELAPSED_TIME_BETWEEN_MESSAGES",
    MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST,
)
def test_elapsed_time_thread_warning_when_no_message_received(consumer_mock):
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
        sut = KafkaConsumer(None, None, None, "topic", group_id="group", bootstrap_servers=["server"])
        assert sut.check_elapsed_time_thread
        alert_time = time.strftime(
            "%Y-%m-%d- %H:%M:%S", time.gmtime(sut.last_received_message_time)
        )
        alert_message = "No new messages in the queue since " + alert_time
        # Make sure the thread woke up at least once
        time.sleep(2 * MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST)
        assert alert_message in buf.getvalue()

    logger.removeHandler(log_handler)


# @patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
# @patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.handles", lambda *a, **k: True)
# @patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.fire", lambda *a, **k: None)
# @patch(
#     "ccx_messaging.consumers.kafka_consumer.get_stringfied_record",
#     lambda *a, **k: None,
# )
# def test_process_message_timeout_no_kafka_requeuer(mock_consumer):
#     """Test timeout mechanism that wraps the process function."""
#     breakpoint()
#     process_message_timeout = 2
#     process_message_timeout_elapsed = 3
#     process_message_timeout_not_elapsed = 1
#     consumer_messages_to_process = _VALID_MESSAGES[0]
#     expected_alert_message = "Couldn't process message in the given time frame."

#     buf = io.StringIO()
#     log_handler = logging.StreamHandler(buf)

#     logger = logging.getLogger()
#     logger.level = logging.DEBUG
#     logger.addHandler(log_handler)

#     with patch("ccx_messaging.consumers.consumer.LOG", logger):
#         sut = KafkaConsumer(None, None, None, None)
#         sut.consumer.consume.return_value = [KafkaMessage(consumer_messages_to_process[0])]
#         assert sut.processing_timeout == 0  # Should be 0 if not changed in config file

#         with patch(
#             "ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process",
#             lambda *a, **k: mock_consumer_process_no_action_catch_exception(0),
#         ):
#             sut.run()
#             assert expected_alert_message not in buf.getvalue()

#         sut.processing_timeout = process_message_timeout

#         with patch(
#             "ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process",
#             lambda *a, **k: mock_consumer_process_no_action_catch_exception(
#                 process_message_timeout_not_elapsed
#             ),
#         ):
#             sut.run()
#             assert expected_alert_message not in buf.getvalue()

#         with patch(
#             "ccx_messaging.consumers.consumer.Consumer.process",
#             lambda *a, **k: mock_consumer_process_no_action_catch_exception(
#                 process_message_timeout_elapsed
#             ),
#         ):
#             sut.run()
#             assert expected_alert_message in buf.getvalue()

#     logger.removeHandler(log_handler)


# _VALID_SERVICES = [("test_service")]

# _VALID_MESSAGES_WITH_UNEXPECTED_SERVICE_HEADER = [
#     ConsumerRecord(
#         topic="platform.upload.announce",
#         partition=0,
#         offset=24,
#         timestamp=1661327909633,
#         timestamp_type=0,
#         key=None,
#         value={
#             "account": "0369233",
#             "category": "archive",
#             "service": "test_service",
#             "timestamp": "2022-08-24T07:58:29.6326987Z",
#         },
#         headers=[("service", b"some_unexpected_service")],
#         checksum=1234,
#         serialized_key_size=12,
#         serialized_value_size=1,
#         serialized_header_size=1,
#     )
# ]

# _VALID_MESSAGES_WITH_EXPECTED_SERVICE_HEADER = [
#     ConsumerRecord(
#         topic="platform.upload.announce",
#         partition=0,
#         offset=24,
#         timestamp=1661327909633,
#         timestamp_type=0,
#         key=None,
#         value={
#             "account": "0369233",
#             "category": "archive",
#             "service": "test_service",
#             "timestamp": "2022-08-24T07:58:29.6326987Z",
#         },
#         headers=[("service", b"test_service")],
#         checksum=1234,
#         serialized_key_size=12,
#         serialized_value_size=1,
#         serialized_header_size=1,
#     )
# ]

# _VALID_MESSAGES_WITH_NO_SERVICE_HEADER = [
#     ConsumerRecord(
#         topic="platform.upload.announce",
#         partition=0,
#         offset=24,
#         timestamp=1661327909633,
#         timestamp_type=0,
#         key=None,
#         value={
#             "account": "0369233",
#             "category": "archive",
#             "service": "test_service",
#             "timestamp": "2022-08-24T07:58:29.6326987Z",
#         },
#         headers=[("some_header", "some_value")],
#         checksum=1234,
#         serialized_key_size=12,
#         serialized_value_size=1,
#         serialized_header_size=1,
#     )
# ]


# def test_anemic_consumer_deserialize():
#     """Test deserialize method of `AnemicConsumer`."""
#     consumer_message = _VALID_MESSAGES[0]
#     buf = io.StringIO()
#     log_handler = logging.StreamHandler(buf)
#     logger = logging.getLogger()
#     logger.level = logging.DEBUG
#     logger.addHandler(log_handler)

#     with patch("ccx_messaging.consumers.consumer.LOG", logger):
#         sut = AnemicConsumer(None, None, None, None, platform_service="any")
#         deserialized = sut.deserialize(consumer_message[0])
#         assert isinstance(deserialized, dict)
#         assert deserialized.get("url") == ""
#         assert (
#             deserialized.get("b64_identity")
#             == "eyJpZGVudGl0eSI6IHsiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICIxMjM0NTY3OCJ9fX0K"
#         )
#         assert deserialized.get("timestamp") == "2020-01-23T16:15:59.478901889Z"

#     logger.removeHandler(log_handler)


# @patch("ccx_messaging.consumers.consumer.Consumer.handles", lambda *a, **k: True)
# @patch("ccx_messaging.consumers.consumer.Consumer.fire", lambda *a, **k: None)
# @patch(
#     "ccx_messaging.consumers.consumer.Consumer.get_stringfied_record",
#     lambda *a, **k: None,
# )
# @pytest.mark.parametrize("service", _VALID_SERVICES)
# def test_anemic_consumer_run_no_service_in_header(service):
#     """Test run method of `AnemicConsumer` with no service in received message's header."""
#     buf = io.StringIO()
#     log_handler = logging.StreamHandler(buf)
#     logger = logging.getLogger()
#     logger.level = logging.DEBUG
#     logger.addHandler(log_handler)

#     with patch("ccx_messaging.consumers.consumer.LOG", logger):
#         sut = AnemicConsumer(None, None, None, None, platform_service=service)
#         sut.consumer = _VALID_MESSAGES_WITH_NO_SERVICE_HEADER
#         sut.run()
#         assert AnemicConsumer.NO_SERVICE_DEBUG_MESSAGE in buf.getvalue()

#     logger.removeHandler(log_handler)


# @pytest.mark.parametrize("service", _VALID_SERVICES)
# def test_anemic_consumer_run_unexpected_service(service):
#     """Test run method of `AnemicConsumer` with unexpected service in received message's header."""
#     buf = io.StringIO()
#     log_handler = logging.StreamHandler(buf)
#     logger = logging.getLogger()
#     logger.level = logging.DEBUG
#     logger.addHandler(log_handler)

#     with patch("ccx_messaging.consumers.consumer.LOG", logger):
#         sut = AnemicConsumer(None, None, None, None, platform_service=service)
#         sut.consumer = _VALID_MESSAGES_WITH_UNEXPECTED_SERVICE_HEADER
#         sut.run()
#         assert (
#             AnemicConsumer.OTHER_SERVICE_DEBUG_MESSAGE.format(b"some_unexpected_service")
#             in buf.getvalue()
#         )

#     logger.removeHandler(log_handler)


# @patch("ccx_messaging.consumers.consumer.Consumer.handles", lambda *a, **k: True)
# @patch("ccx_messaging.consumers.consumer.Consumer.fire", lambda *a, **k: None)
# @patch(
#     "ccx_messaging.consumers.consumer.Consumer.get_stringfied_record",
#     lambda *a, **k: None,
# )
# @pytest.mark.parametrize("service", _VALID_SERVICES)
# def test_anemic_consumer_run_expected_service(service):
#     """Test run method of `AnemicConsumer` with expected service in received message's header."""
#     buf = io.StringIO()
#     log_handler = logging.StreamHandler(buf)
#     logger = logging.getLogger()
#     logger.level = logging.DEBUG
#     logger.addHandler(log_handler)

#     with patch("ccx_messaging.consumers.consumer.LOG", logger):
#         sut = AnemicConsumer(None, None, None, None, platform_service=service)
#         sut.consumer = _VALID_MESSAGES_WITH_EXPECTED_SERVICE_HEADER
#         sut.run()
#         logs = buf.getvalue()
#         assert AnemicConsumer.OTHER_SERVICE_DEBUG_MESSAGE not in logs
#         assert AnemicConsumer.NO_SERVICE_DEBUG_MESSAGE not in logs
#         assert AnemicConsumer.EXPECTED_SERVICE_DEBUG_MESSAGE in logs
#     logger.removeHandler(log_handler)
