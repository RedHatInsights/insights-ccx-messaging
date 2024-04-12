#!/bin/bash

CICD_TOOLS_URL="https://raw.githubusercontent.com/RedHatInsights/cicd-tools/main/src/bootstrap.sh"
# shellcheck source=/dev/null
source <(curl -sSL "$CICD_TOOLS_URL") image_builder

export CICD_IMAGE_BUILDER_IMAGE_NAME='quay.io/cloudservices/insights-ccx-messaging'
export CICD_IMAGE_BUILDER_ADDITIONAL_TAGS=("latest")

cicd::image_builder::build_and_push
