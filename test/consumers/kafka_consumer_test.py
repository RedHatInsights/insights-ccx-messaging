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

import datetime
import logging
import io
import time
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException
from freezegun import freeze_time

from ccx_messaging.consumers.kafka_consumer import KafkaConsumer
from ccx_messaging.error import CCXMessagingError

from . import KafkaMessage


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

_MESSAGE_WITHOUT_IDENTITY = {
    "account": "12345678",
    "url": "any/url",
    "timestamp": "2020-01-23T16:15:59.478901889Z",
    "cluster_name": None,
}


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_str(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = KafkaConsumer(None, None, None, None)
    assert sut.deserialize(KafkaMessage(msg)) == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_bytes(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = KafkaConsumer(None, None, None, None)
    assert sut.deserialize(KafkaMessage(msg.encode())) == value


@pytest.mark.parametrize("msg,value", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_deserialize_valid_bytearray(msg, value):
    """Test that proper string JSON input messages are correctly deserialized."""
    sut = KafkaConsumer(None, None, None, None)
    assert sut.deserialize(KafkaMessage(bytearray(msg.encode()))) == value


PLATFORM_SERVICE = "some_service"
_NO_HANDLE_HEADERS = [
    None,
    {},
    {"not_expected_key": b"ignored_value"},
    {"service": b"this is not my service"},
]

_HANDLE_HEADER = {"service": b"some_service"}


@pytest.mark.parametrize("headers", _NO_HANDLE_HEADERS)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_platform_filter(headers):
    """Test that filter by platform behaves as expected."""
    sut = KafkaConsumer(None, None, None, None, platform_service=PLATFORM_SERVICE)
    assert not sut.handles(KafkaMessage("{}", headers))


@pytest.mark.parametrize("headers", _NO_HANDLE_HEADERS + [_HANDLE_HEADER])
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_platform_filter_no_platform_service(headers):
    """Test that filter by platform behaves as expected."""
    sut = KafkaConsumer(None, None, None, None)
    assert sut.handles(KafkaMessage("{}", headers))


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_platform_filter_platform_service():
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
        with patch("os.environ", new={}):
            kwargs = {
                "group.id": group,
                "bootstrap.servers": server,
            }
            KafkaConsumer(None, None, None, topic, **kwargs)
            config = {
                "bootstrap.servers": server,
                "group.id": group,
            }
            mock_consumer_init.assert_called_with(config)


MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST = 2


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
@patch(
    "ccx_messaging.consumers.kafka_consumer.MAX_ELAPSED_TIME_BETWEEN_MESSAGES",
    MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST,
)
def test_elapsed_time_thread_no_warning_when_message_received():
    """Test elapsed time thread if new message received on time.

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


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
@patch(
    "ccx_messaging.consumers.kafka_consumer.MAX_ELAPSED_TIME_BETWEEN_MESSAGES",
    MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST,
)
def test_elapsed_time_thread_warning_when_no_message_received():
    """Test elapsed time thread if no new message received on time.

    Test that warnings are sent if no new messages are received before
    the defined MAX_ELAPSED_TIME_BETWEEN_MESSAGES.
    """
    buf = io.StringIO()
    log_handler = logging.StreamHandler(buf)

    logger = logging.getLogger()
    logger.level = logging.DEBUG
    logger.addHandler(log_handler)

    with patch("ccx_messaging.consumers.kafka_consumer.LOG", logger):
        sut = KafkaConsumer(
            None, None, None, "topic", group_id="group", bootstrap_servers=["server"]
        )
        assert sut.check_elapsed_time_thread
        alert_message = "No new messages in the queue"
        # Make sure the thread woke up at least once
        time.sleep(2 * MAX_ELAPSED_TIME_BETWEEN_MESSAGES_TEST)
        assert alert_message in buf.getvalue()

    logger.removeHandler(log_handler)


@patch(
    "ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *args, **kwargs: MagicMock()
)
def test_handles_old_message():
    """Check that too old message is discarded."""
    msg = KafkaMessage(b"", None, 0)
    sut = KafkaConsumer(
        None,
        None,
        None,
        "topic",
        max_record_age=100,
        group_id="group",
        bootstrap_servers=["server"],
    )

    assert not sut.handles(msg)

    msg = KafkaMessage(b"", None, time.time() * 1000)  # using ms
    assert sut.handles(msg)


@patch(
    "ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *args, **kwargs: MagicMock()
)
def test_handles_disable_age_message_filtering():
    """Check that too old message is discarded."""
    msg = KafkaMessage(b"", None, 0)
    sut = KafkaConsumer(
        None,
        None,
        None,
        "topic",
        max_record_age=-1,
        group_id="group",
        bootstrap_servers=["server"],
    )

    assert sut.handles(msg)


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_process_msg_not_handled():
    """Test the `process_msg` method in the `KafkaConsumer` class.

    This method will check that neither sut.deserialize or sut.process are called
    """
    sut = KafkaConsumer(None, None, None, None)
    sut.deserialize = MagicMock()
    sut.process = MagicMock()
    sut.handles = lambda *a: False

    sut.process_msg(None)
    assert not sut.deserialize.called
    assert not sut.process.called

    sut.process_msg(KafkaMessage("any message that won't be handled"))
    assert not sut.deserialize.called
    assert not sut.process.called


@pytest.mark.parametrize("value,expected", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
@patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.handles", lambda *a, **k: True)
@patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.fire", lambda *a, **k: None)
def test_process_msg_handled(value, expected):
    """Check if `process_msg` behaves as expected."""
    sut = KafkaConsumer(None, None, None, None)
    input_msg = KafkaMessage(value)

    with patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process") as process_mock:
        sut.process_msg(input_msg)
        process_mock.assert_called_with(expected)


@pytest.mark.parametrize("value,expected", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
@patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.handles", lambda *a, **k: True)
@patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.fire", lambda *a, **k: None)
def test_non_processed_to_dlq(value, expected):
    """Check that, if in some point an exception is raised, DLQ will handle it."""
    sut = KafkaConsumer(None, None, None, None)
    input_msg = KafkaMessage(value)

    with patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process") as process_mock:
        with patch(
            "ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process_dead_letter"
        ) as process_dlq_mock:
            process_mock.side_effect = [CCXMessagingError, TimeoutError, IndexError]
            sut.process_msg(input_msg)
            process_dlq_mock.assert_called_with(input_msg)

            sut.process_msg(input_msg)
            process_dlq_mock.assert_called_with(input_msg)

            sut.process_msg(input_msg)
            process_dlq_mock.assert_called_with(input_msg)


@pytest.mark.parametrize("value,expected", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_process_dead_letter_no_configured(value, expected):
    """Check that process_dead_letter method works as expected."""
    sut = KafkaConsumer(None, None, None, None)
    sut.dlq_producer = MagicMock()  # inject a mock producer
    sut.dlq_producer.__bool__.return_value = False

    input_message = KafkaMessage(value)

    sut.process_dead_letter(input_message)
    assert not sut.dlq_producer.produce.called


@pytest.mark.parametrize("value,expected", _VALID_MESSAGES)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
@patch("ccx_messaging.consumers.kafka_consumer.Producer")
def test_process_dead_letter_message(producer_init_mock, value, expected):
    """Check behaviour when DLQ is properly configured."""
    dlq_topic_name = "dlq_topic"
    producer_mock = MagicMock()
    producer_init_mock.return_value = producer_mock

    sut = KafkaConsumer(None, None, None, None, dead_letter_queue_topic=dlq_topic_name)
    assert producer_init_mock.called

    message_mock = MagicMock()
    message_mock.value.return_value = value

    sut.process_dead_letter(message_mock)
    producer_mock.produce.assert_called_with(dlq_topic_name, value)


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_run_success(consumer_init_mock):
    """Check the run method process the message as many times as needed."""
    consumer_mock = MagicMock()
    consumer_init_mock.return_value = consumer_mock

    consumer_mock.consume.side_effect = [
        [KafkaMessage(b"0")],
        [KafkaMessage(b"1"), KafkaMessage(b"2")],
        KeyboardInterrupt(),
    ]

    sut = KafkaConsumer(None, None, None, "topic")
    sut.process_msg = MagicMock()
    sut.run()

    assert consumer_mock.consume.call_count == 3
    assert consumer_mock.close.call_count == 1
    assert sut.process_msg.call_count == 3


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer")
def test_run_fail(consumer_init_mock):
    """Check the run method process the message as many times as needed."""
    consumer_mock = MagicMock()
    consumer_init_mock.return_value = consumer_mock

    consumer_mock.consume.side_effect = [
        KafkaException(),
    ]

    sut = KafkaConsumer(None, None, None, "topic")
    sut.process_msg = MagicMock()
    sut.run()

    assert consumer_mock.consume.call_count == 1
    assert consumer_mock.close.call_count == 1
    assert not sut.process_msg.called


@pytest.mark.parametrize(
    "deserialized_msg", [msg[1] for msg in _VALID_MESSAGES] + [_MESSAGE_WITHOUT_IDENTITY]
)
@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_broker_creation(deserialized_msg):
    """Check that create_broker returns the expected values."""
    sut = KafkaConsumer(None, None, None, "topic")
    broker = sut.create_broker(deserialized_msg)
    msg_timestamp = datetime.datetime.fromisoformat(deserialized_msg["timestamp"])

    assert broker["original_path"] == deserialized_msg["url"]
    identity = (
        deserialized_msg.get("identity", {})
        .get("identity", {})
        .get("internal", {})
        .get("org_id", None)
    )
    assert broker["org_id"] == identity
    assert broker["year"] == f"{msg_timestamp.year:04}"
    assert broker["month"] == f"{msg_timestamp.month:02}"
    assert broker["day"] == f"{msg_timestamp.day:02}"
    assert broker["hour"] == f"{msg_timestamp.hour:02}"
    assert broker["minute"] == f"{msg_timestamp.minute:02}"
    assert broker["second"] == f"{msg_timestamp.second:02}"
    assert broker["time"] == (
        f"{msg_timestamp.hour:02}{msg_timestamp.minute:02}{msg_timestamp.second:02}"
    )


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
@patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.handles", lambda *a, **k: True)
@patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.fire", lambda *a, **k: None)
def test_last_received_message_time_is_updated():
    """Check that the variable last_received_message_time used by a Thread is correctly updated.

    [CCXDEV-14812] the variable is never updated
    """
    t1 = datetime.datetime(2025, 1, 31, 12, 0, 0, tzinfo=datetime.timezone.utc)
    t2 = datetime.datetime(2025, 1, 31, 12, 20, 0, tzinfo=datetime.timezone.utc)

    with freeze_time(t1) as frozen_time:
        sut = KafkaConsumer(None, None, None, None)
        input_msg = KafkaMessage("{}")

        frozen_time.move_to(t2)

        with patch("ccx_messaging.consumers.kafka_consumer.KafkaConsumer.process", lambda: None):
            sut.process_msg(input_msg)

        assert sut.last_received_message_time == t2.timestamp()
