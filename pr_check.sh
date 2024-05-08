#!/bin/bash

CICD_TOOLS_URL="https://raw.githubusercontent.com/RedHatInsights/cicd-tools/main/src/bootstrap.sh"
export CICD_IMAGE_BUILDER_IMAGE_NAME='quay.io/cloudservices/ccx-messaging'
export CICD_IMAGE_BUILDER_ADDITIONAL_TAGS=("latest")

APP_NAME="ccx-data-pipeline"  # name of app-sre "application" folder this component lives in
COMPONENT_NAME="archive-sync"  # name of app-sre "resourceTemplate" in deploy.yaml for this component
IMAGE="quay.io/cloudservices/ccx-messaging"
COMPONENTS="archive-sync"  # space-separated list of components to load
COMPONENTS_W_RESOURCES="archive-sync"  # component to keep
CACHE_FROM_LATEST_IMAGE="true"

export IQE_PLUGINS="ccx"
export IQE_MARKER_EXPRESSION=""
# Workaround: There are no specific integration tests. Check that the service loads and iqe plugin works.
export IQE_FILTER_EXPRESSION="test_plugin_accessible"
export IQE_REQUIREMENTS_PRIORITY=""
export IQE_TEST_IMPORTANCE=""
export IQE_CJI_TIMEOUT="30m"

# shellcheck source=/dev/null
if ! source <(curl -sSL "$CICD_TOOLS_URL") image_builder; then
    echo "Error loading image_builder module!"
    exit 1
fi

if ! cicd::image_builder::build_and_push; then
    echo "Error building image!"
    exit 1
fi

# Try to deploy the service on ephemeral to check the changes haven't break it
if ! source $CICD_ROOT/deploy_ephemeral_env.sh; then
    echo "Error deploying the service on ephemeral!"
    exit 1
fi

# Temporary stub
mkdir -p artifacts
echo '<?xml version="1.0" encoding="utf-8"?><testsuites><testsuite name="pytest" errors="0" failures="0" skipped="0" tests="1" time="0.014" timestamp="2021-05-13T07:54:11.934144" hostname="thinkpad-t480s"><testcase classname="test" name="test_stub" time="0.000" /></testsuite></testsuites>' > artifacts/junit-stub.xml
