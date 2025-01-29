#!/bin/bash
# Rebuilds the Quesma Docker image and tags it as quesma:nightly
set -e
cd "$(dirname "$0/")/.."

QUESMA_BUILD_SHA=$(git rev-parse HEAD)
QUESMA_BUILD_DATE=$(git --no-pager log -1 --date=format:'%Y-%m-%d' --format="%ad")
QUESMA_VERSION="development"

export DOCKER_BUILDKIT=1

docker build --build-arg QUESMA_BUILD_DATE="$QUESMA_BUILD_DATE" --build-arg QUESMA_VERSION="$QUESMA_VERSION" --build-arg QUESMA_BUILD_SHA="$QUESMA_BUILD_SHA" -f quesma/Dockerfile -t quesma/quesma:nightly quesma
