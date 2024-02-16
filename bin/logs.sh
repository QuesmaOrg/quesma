#!/bin/bash
# Shows logs in tail mode for the local development environment.
set -e
cd "$(dirname "$0/")/.."

docker compose -f docker/local-dev.yml logs -f
