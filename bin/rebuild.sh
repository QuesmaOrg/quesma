#!/bin/bash
# Rebuilds the Quesma proxy and restart the service.
# Assumes your local env was run by bin/up.sh
set -e
cd "$(dirname "$0/")/.."

docker compose -f docker/local-dev.yml build quesma && \
  docker compose -f docker/local-dev.yml stop quesma && \
  docker compose -f docker/local-dev.yml up -d --no-deps quesma
