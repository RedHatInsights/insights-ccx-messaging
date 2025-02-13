# Copyright 2025 Red Hat, Inc
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

import pytest
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaException

from ccx_messaging.consumers.rules_results_consumer import RulesResultsConsumer

from . import KafkaMessage


@patch("ccx_messaging.consumers.kafka_consumer.ConfluentConsumer", lambda *a, **k: MagicMock())
def test_process_msg_raise_error():
    """Test when the `process` message raises an error."""
    sut = RulesResultsConsumer(None, None, None, incoming_topic=None)
    with pytest.raises(KafkaException):
        sut.process_msg(KafkaMessage(error=True))