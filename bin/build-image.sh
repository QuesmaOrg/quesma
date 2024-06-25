#!/bin/bash
# Rebuilds the Quesma Docker image and tags it as quesma:latest
set -e
cd "$(dirname "$0/")/.."
source bin/lib.sh

docker build -f quesma/Dockerfile -t quesma:latest quesma
