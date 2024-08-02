#!/bin/bash

set -e

readonly REPOSITORY="https://github.com/quesmaOrg/quesma"

git clone -q --depth=1 "$REPOSITORY"

cd quesma

docker-compose -f examples/kibana-sample-data/docker-compose.yml up
