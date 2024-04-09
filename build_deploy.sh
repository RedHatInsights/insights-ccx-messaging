#!/bin/bash

set -exv

IMAGE="quay.io/cloudservices/ccx-messaging"
IMAGE_TAG=$(git rev-parse --short=7 HEAD)

if [[ -z "$QUAY_USER" || -z "$QUAY_TOKEN" ]]; then
    echo "QUAY_USER and QUAY_TOKEN must be set"
    exit 1
fi

CLEANUP_RAN=''
cleanup() {
    if [[ -z "$CLEANUP_RAN" ]]; then
        rm -fr "$DOCKER_CONFIG"
        CLEANUP_RAN='true'
    fi
}

DOCKER_CONFIG=$(mktemp -d -p "$HOME" -t "jenkins-${JOB_NAME}-${BUILD_NUMBER}-XXXXXX")
export DOCKER_CONFIG
mkdir -p "$DOCKER_CONFIG"
trap cleanup EXIT ERR SIGINT SIGTERM

docker login -u="$QUAY_USER" --password-stdin quay.io <<< "$QUAY_TOKEN"
docker build -t "${IMAGE}:${IMAGE_TAG}" -t "${IMAGE}:latest" .
docker push "${IMAGE}:${IMAGE_TAG}"
docker push "${IMAGE}:latest"
