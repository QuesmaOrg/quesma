#!/bin/bash
# Rebuilds the Quesma proxy and restart the service.
# Assumes your local env was run by bin/up.sh
set -e
cd "$(dirname "$0/")/.."
source bin/lib.sh

docker compose  -f "$QUESMA_COMPOSE_FILE" build "$QUESMA_COMPOSE_FILE" build --build-arg QUESMA_BUILD_DATE="$QUESMA_BUILD_DATE" --build-arg QUESMA_VERSION="$QUESMA_VERSION" --build-arg QUESMA_BUILD_SHA="$QUESMA_BUILD_SHA" quesma && \
  docker compose -f "$QUESMA_COMPOSE_FILE" stop quesma && \
  docker compose -f "$QUESMA_COMPOSE_FILE" up --no-deps quesma
