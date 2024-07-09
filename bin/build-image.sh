#!/bin/bash
# Rebuilds the Quesma Docker image and tags it as quesma:latest
set -e
docker build -f quesma/Dockerfile -t quesma:latest quesma
