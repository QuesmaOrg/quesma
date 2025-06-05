#!/bin/bash
# Run integration tests
set -e
cd "$(dirname "$0/")/.."

bin/build-image.sh

cd ci/it

if [ -n "$1" ]; then
  # Run only tests matching the pattern
  go test -v -run "$1"
else
  # Run all tests
  go test -v
fi
