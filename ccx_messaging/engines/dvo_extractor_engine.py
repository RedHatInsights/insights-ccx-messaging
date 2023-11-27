# Copyright 2023 Red Hat Inc.
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

"""Module that defines an Engine class for processing DVO metrics."""

import json
import logging
import os.path
import tempfile

from insights.core.archives import extract
from insights.core.hydration import initialize_broker
from insights_messaging.engine import Engine as ICMEngine

log = logging.getLogger(__name__)


class DVOExtractorEngine(ICMEngine):

    """Engine for extraction of features important for DVO processing."""

    def process(self, broker, path):
        """Retrieve DVO metrics from a downloaded archive.

        The archive is extracted and if the DVO metrics are found, the JSON with relevant
        info is created and returned as the method output. Otherwise, None is returned.
        """
        for watcher in self.watchers:
            watcher.watch_broker(broker)

        try:
            log.debug(tempfile.gettempdir())
            self.fire("pre_extract", broker, path)

            with extract(
                path, timeout=self.extract_timeout, extract_dir=self.extract_tmp_dir
            ) as extraction:
                ctx, broker = initialize_broker(extraction.tmp_dir, broker=broker)

                self.fire("on_extract", ctx, broker, extraction)

                try:
                    filename = os.path.join(extraction.tmp_dir, "config", "dvo_metrics")
                    results = []
                    with open(filename, encoding="utf-8") as stream:
                        dvo_metrics = stream.read()
                        log.debug("DVO metrics found, starting parsing")
                        dvo_metrics = dvo_metrics.split("\n")
                        for metric in dvo_metrics:
                            if metric == "":
                                continue
                            try:
                                fields = {}
                                # parse the string with field values into dictionary
                                for attr in metric[metric.find("{") + 1: metric.find("}")].split(
                                    ","
                                ):
                                    fields[attr.split("=")[0]] = attr.split("=")[1][1:-1]

                                # construct the expected data format
                                result = {
                                    "type": metric.split("{")[0][
                                        len("deployment_validation_operator_"):
                                    ],
                                    "kind": fields["kind"],
                                    "namespace_uid": fields["namespace_uid"],
                                    "uid": fields["uid"],
                                    "namespace": fields.get("namespace", ""),
                                    "name": fields.get("name", ""),
                                }
                                results.append(result)
                            except Exception:
                                log.debug("metric could not be processed; skipping")
                        log.debug("starting publishing process")
                        self.fire("on_engine_success", broker, result)
                        return json.dumps({"results": results})
                except FileNotFoundError:
                    log.debug("archive does not contain DVO metrics; skipping")
                    return None

        except Exception as ex:
            self.fire("on_engine_failure", broker, ex)
            raise
        finally:
            self.fire("on_engine_complete", broker)
