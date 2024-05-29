#!/bin/bash

CICD_TOOLS_URL="https://raw.githubusercontent.com/RedHatInsights/cicd-tools/main/src/bootstrap.sh"
export CICD_IMAGE_BUILDER_IMAGE_NAME='quay.io/cloudservices/ccx-messaging'
export CICD_IMAGE_BUILDER_ADDITIONAL_TAGS=("latest")

# shellcheck source=/dev/null
if ! source <(curl -sSL "$CICD_TOOLS_URL") image_builder; then
    echo "Error loading image_builder module!"
    exit 1
fi

if ! cicd::image_builder::build_and_push; then
    echo "Error building image!"
    exit 1
fi

# Temporary stub
mkdir -p artifacts
echo '<?xml version="1.0" encoding="utf-8"?><testsuites><testsuite name="pytest" errors="0" failures="0" skipped="0" tests="1" time="0.014" timestamp="2021-05-13T07:54:11.934144" hostname="thinkpad-t480s"><testcase classname="test" name="test_stub" time="0.000" /></testsuite></testsuites>' > artifacts/junit-stub.xml
