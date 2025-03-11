#!/bin/bash
# Run integration tests
set -e
cd "$(dirname "$0/")/.."

bin/build-image.sh

cd ci/it

go test -v
