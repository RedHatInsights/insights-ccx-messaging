# Copyright 2024 Red Hat Inc.
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
import os
from io import StringIO

from insights.core import dr
from insights.core.archives import extract
from insights.core.hydration import initialize_broker
from insights_messaging.engine import Engine as ICMEngine

log = logging.getLogger(__name__)


class OCPEngine(ICMEngine):
    """Extract, download and process IO archive."""

    def process(self, broker, path):
        """Get results from applying Insights rules to the downloaded archive.

        The archive is extracted and processed if the `openshift_lightspeed.json`
        file is not found within its root directory. The JSON resulting from applying
        the Insights rules is retrieved and returned as the method's output. Otherwise,
        None is returned.
        """
        for w in self.watchers:
            w.watch_broker(broker)

        try:
            self.fire("pre_extract", broker, path)
            with extract(
                path, timeout=self.extract_timeout, extract_dir=self.extract_tmp_dir
            ) as extraction:
                ctx, broker = initialize_broker(extraction.tmp_dir, broker=broker)
                self.fire("on_extract", ctx, broker, extraction)

                ols_file = os.path.join(extraction.tmp_dir, "openshift_lightspeed.json")
                if os.path.exists(ols_file):
                    log.debug("archive contains openshift_lightspeed.json file; skipping")
                    return "{}"

                output = StringIO()
                with self.Formatter(broker, stream=output):
                    dr.run_components(self.target_components, self.components_dict, broker=broker)
                output.seek(0)
                result = output.read()
                self.fire("on_engine_success", broker, result)
                return result
        except Exception as ex:
            self.fire("on_engine_failure", broker, ex)
            raise
        finally:
            self.fire("on_engine_complete", broker)
