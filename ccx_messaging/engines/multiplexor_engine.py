# Copyright 2024 Red Hat, Inc
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

"""Module that defines an Engine class for selecting where to publish a received archive."""

import logging
import tarfile

from ccx_messaging.error import CCXMessagingError

from insights.core.dr import Broker
from insights.formats import Formatter
from insights_messaging.engine import Engine


class MultiplexorEngine(Engine):
    """Engine that will create a report with the identified content of the archive."""

    def __init__(
        self,
        formatter: Formatter,
        target_components: list,
        extract_timeout: int | None = None,
        unpacked_archive_size_limit: int | None = None,
        extract_tmp_dir: str | None = None,
        filters: dict[str, str] | None = None,
    ):
        """Initialise the engine with the given filters.

        The `filter` argument is a dictionary where the keys are a path and the value, a mark.
        If a file with the given path is present inside the archive, it will be noted with
        the given "mark". An archive could have several matching files, so it could get a bunch
        of different marks.
        """
        super().__init__(
            formatter=formatter,
            target_components=target_components,
            extract_timeout=extract_timeout,
            unpacked_archive_size_limit=unpacked_archive_size_limit,
            extract_tmp_dir=extract_tmp_dir,
        )
        self.filters = filters if filters is not None else {}
        self.logger = logging.getLogger(__name__)

    def process(self, broker: Broker, path: str) -> set[str]:
        """Open an archive to check its content and classify it according to filters."""
        self.logger.info("Opening %s for multiplexing", path)
        self.fire("pre_extract", broker, path)

        try:
            with tarfile.open(path) as tf:
                self.fire("on_extract", None, broker, tf)

                filenames = tf.getnames()

                marks = set()
                for file_to_find, mark in self.filters.items():
                    if file_to_find in filenames:
                        marks.add(mark)

            if not marks:
                marks.add("DEFAULT")

        except tarfile.ReadError as ex:
            raise CCXMessagingError(
                "ReadError reading the archive", additional_data={"archive_path": path}
            ) from ex

        self.logger.info("Reporting marks: %s", marks)
        self.fire("on_engine_success", broker, marks)
        return marks
