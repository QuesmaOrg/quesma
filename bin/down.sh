#!/bin/bash
# Stops the local development environment, started by up.sh
set -e
cd "$(dirname "$0")/.."

docker compose -f docker/local-dev.yml down