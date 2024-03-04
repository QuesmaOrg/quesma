#!/bin/bash
# Shows logs in tail mode for the local development environment.
set -e
cd "$(dirname "$0/")/.."
source bin/lib.sh

docker compose -f "$QUESMA_COMPOSE_FILE" logs -f
