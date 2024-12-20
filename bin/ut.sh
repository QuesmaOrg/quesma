#!/bin/bash
# Run unit tests
set -e
cd "$(dirname "$0/")/../quesma"

go run gotest.tools/gotestsum@latest --format pkgname-and-test-fails ./...
