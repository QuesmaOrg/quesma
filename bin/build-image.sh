#!/bin/bash
# Rebuilds the Quesma Docker image and tags it as quesma:nightly
set -e
cd "$(dirname "$0/")/.."
docker build -f quesma/Dockerfile -t quesma/quesma:nightly quesma
