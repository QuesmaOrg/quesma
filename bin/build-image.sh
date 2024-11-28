#!/bin/bash
# Rebuilds the Quesma Docker image and tags it as quesma:nightly
set -e
cd "$(dirname "$0/")/.."

QUESMA_BUILD_SHA=$(git rev-parse HEAD)
QUESMA_BUILD_DATE=$(date -u +"%Y-%m-%d %H:%M:%S")
QUESMA_VERSION="development"

docker build --build-arg QUESMA_BUILD_DATE="$QUESMA_BUILD_DATE" --build-arg QUESMA_VERSION="$QUESMA_VERSION" --build-arg QUESMA_BUILD_SHA="$QUESMA_BUILD_SHA" -f quesma/Dockerfile -t quesma/quesma:nightly quesma
