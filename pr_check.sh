#!/bin/bash
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

set -exv


# --------------------------------------------
# Options that must be configured by app owner
# --------------------------------------------
APP_NAME="ccx-data-pipeline"  # name of app-sre "application" folder this component lives in
COMPONENT_NAME="multiplexor archive-sync archive-sync-ols archive-sync-ols-copy rules-uploader"  # name of app-sre "resourceTemplate" in deploy.yaml for this component
IMAGE="quay.io/cloudservices/ccx-messaging"
COMPONENTS="multiplexor archive-sync archive-sync-ols archive-sync-ols-copy rules-processing rules-uploader parquet-factory"  # space-separated list of components to load
COMPONENTS_W_RESOURCES=""  # component to keep
CACHE_FROM_LATEST_IMAGE="true"
DEPLOY_FRONTENDS="false"
# Set the correct images for pull requests.
# pr_check in pull requests still uses the old cloudservices images
EXTRA_DEPLOY_ARGS="\
    --set-parameter multiplexor/IMAGE=quay.io/cloudservices/ccx-messaging \
    --set-parameter archive-sync/IMAGE=quay.io/cloudservices/ccx-messaging \
    --set-parameter archive-sync-ols/IMAGE=quay.io/cloudservices/ccx-messaging \
    --set-parameter archive-sync-ols-copy/IMAGE=quay.io/cloudservices/ccx-messaging \
    --set-parameter rules-uploader/IMAGE=quay.io/cloudservices/ccx-messaging
"

export IQE_PLUGINS="ccx"
# Run all pipeline and ui tests
export IQE_MARKER_EXPRESSION="internal"
export IQE_FILTER_EXPRESSION=""
export IQE_REQUIREMENTS_PRIORITY=""
export IQE_TEST_IMPORTANCE=""
export IQE_CJI_TIMEOUT="30m"
export IQE_SELENIUM="false"
export IQE_ENV="ephemeral"
export IQE_ENV_VARS="DYNACONF_USER_PROVIDER__rbac_enabled=false"


function run_tests() {
    # component name needs to be re-export to match ClowdApp name (as bonfire requires for this)
    export COMPONENT_NAME="archive-sync"
    source $CICD_ROOT/cji_smoke_test.sh
    source $CICD_ROOT/post_test_results.sh  # publish results in Ibutsu
}

# Install bonfire repo/initialize
CICD_URL=https://raw.githubusercontent.com/RedHatInsights/bonfire/master/cicd
curl -s $CICD_URL/bootstrap.sh > .cicd_bootstrap.sh && source .cicd_bootstrap.sh
echo "creating PR image"
source $CICD_ROOT/build.sh

echo "deploying to ephemeral"
source $CICD_ROOT/deploy_ephemeral_env.sh

echo "running PR tests"
run_tests
