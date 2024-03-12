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

"""Module that defines an Engine class for processing a downloaded archive."""

import logging
import os.path

from insights.core.archives import extract
from insights.core.hydration import initialize_broker
from insights_messaging.engine import Engine as ICMEngine

log = logging.getLogger(__name__)


class SHAExtractorEngine(ICMEngine):
    """Engine for extraction of downloading tar archive and selecting a file to be processed."""

    def process(self, broker, path):
        """Retrieve SHA records from a downloaded archive.

        The archive is extracted and if the SHA records are found, the JSON is retrieved
        from the file and returned as the method output. Otherwise, None is returned.
        """
        for watcher in self.watchers:
            watcher.watch_broker(broker)

        try:
            self.fire("pre_extract", broker, path)

            with extract(
                path, timeout=self.extract_timeout, extract_dir=self.extract_tmp_dir
            ) as extraction:
                ctx, broker = initialize_broker(extraction.tmp_dir, broker=broker)

                self.fire("on_extract", ctx, broker, extraction)

                try:
                    filename = os.path.join(extraction.tmp_dir, "config", "workload_info.json")
                    with open(filename, encoding="utf-8") as stream:
                        result = stream.read()
                        log.debug("workload info found, starting publishing process")
                        self.fire("on_engine_success", broker, result)
                        return result
                except FileNotFoundError:
                    log.debug("archive does not contain workload info; skipping")
                    return None

        except Exception as ex:
            self.fire("on_engine_failure", broker, ex)
            raise
        finally:
            self.fire("on_engine_complete", broker)
