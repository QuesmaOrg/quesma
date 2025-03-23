#!/bin/bash
# Run unit tests
set -e
cd "$(dirname "$0/")/../platform"

go run gotest.tools/gotestsum@latest --format pkgname-and-test-fails ./...
