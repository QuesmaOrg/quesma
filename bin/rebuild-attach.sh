#!/bin/bash
# Rebuilds the Quesma proxy and restart the service.
# Assumes your local env was run by bin/up.sh
set -e
cd "$(dirname "$0/")/.."
source bin/lib.sh

docker compose  -f "$QUESMA_COMPOSE_FILE" build ${DOCKER_COMPOSE_BUILD_ARGS} quesma && \
  docker compose -f "$QUESMA_COMPOSE_FILE" stop quesma && \
  docker compose -f "$QUESMA_COMPOSE_FILE" up --no-deps quesma
