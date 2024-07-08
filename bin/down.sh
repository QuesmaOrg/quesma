#!/bin/bash
# Stops the local development environment, started by up.sh
set -e
cd "$(dirname "$0")/.."
source bin/lib.sh

docker compose -f "$QUESMA_COMPOSE_FILE" down
remove_status_file