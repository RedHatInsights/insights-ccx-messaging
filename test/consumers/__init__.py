"""Module containing tests for the consumers."""

from confluent_kafka import TIMESTAMP_NOT_AVAILABLE


class KafkaMessage:
    """Test double for the confluent_kafka.Message class."""

    def __init__(self, msg=None, headers=None, timestamp=None, error=False):
        """Initialize a KafkaMessage test double."""
        self.msg = msg
        self._headers = headers
        self._timestamp = timestamp
        self.topic = lambda: "topic"
        self.partition = lambda: 0
        self.offset = lambda: 0
        self.value = lambda: self.msg
        if error:
            self.error = lambda: "error"
        else:
            self.error = lambda: None

        self.headers = lambda: self._headers

    def timestamp(self):
        """Test double for the Message.timestamp function."""
        if self._timestamp is None:
            return TIMESTAMP_NOT_AVAILABLE, None

        else:
            return None, self._timestamp
