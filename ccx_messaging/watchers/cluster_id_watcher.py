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

"""Module including a mixed watcher to retrieve the cluster ID from the extracted file."""

import logging
import os
from uuid import UUID

from insights_messaging.watchers import ConsumerWatcher, EngineWatcher


LOG = logging.getLogger(__name__)


class ClusterIdWatcher(EngineWatcher, ConsumerWatcher):
    """Mixed `Watcher` that is able to watch both `Consumer` and `Engine`."""

    CLUSTER_ID_LENGTH = 36

    def __init__(self):
        """Initialize a `ClusterIdWatcher`."""
        self.last_record = None

    def on_recv(self, input_msg):
        """Get and stores the `ConsumerRecord` when a new Kafka record arrives."""
        self.last_record = input_msg

    def on_extract(self, ctx, broker, extraction):
        """Receive the notification when the file is extracted.

        The method get the directory where the files are extracted and find the
        id in the expected path.
        """
        if self.last_record is None:
            LOG.warning(
                "Unexpected data flow: watched extraction event without a previous receiving event"
            )
            return

        if self.last_record.get("cluster_name", None) is not None:
            return

        id_file_path = os.path.join(extraction.tmp_dir, "config", "id")

        try:
            with open(id_file_path) as id_file:
                cluster_uuid = id_file.read(ClusterIdWatcher.CLUSTER_ID_LENGTH)

                try:
                    UUID(cluster_uuid)
                    self.last_record["cluster_name"] = cluster_uuid

                except ValueError:
                    self.last_record["cluster_name"] = None
                    LOG.warning("The cluster id is not an UUID. Skipping its extraction")

        except FileNotFoundError:
            self.last_record["cluster_name"] = None
            LOG.warning(
                "The archive doesn't contain a valid Cluster Id file. Skipping its extraction"
            )

        except OSError:
            self.last_record["cluster_name"] = None
            LOG.warning(f"Could not read file: {id_file_path}")

        finally:
            self.last_record = None
