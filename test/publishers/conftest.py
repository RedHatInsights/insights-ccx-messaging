"""Shared fixtures for publisher tests."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _mock_kafka_producer(request, monkeypatch):
    """Prevent real confluent_kafka.Producer creation in publisher tests.

    The real Producer spawns librdkafka background threads that block for ~4 seconds
    on destruction when the broker is unreachable, causing the test suite to take
    ~10 minutes instead of seconds.

    Tests that need the real Producer (e.g. to verify config validation) can opt out
    with @pytest.mark.real_kafka_producer.
    """
    if "real_kafka_producer" not in request.keywords:
        monkeypatch.setattr(
            "ccx_messaging.publishers.kafka_publisher.Producer", lambda *a, **kw: MagicMock()
        )
