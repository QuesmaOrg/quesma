#!/bin/bash -e

for gomod in **/go.mod ; do
  pushd "$(dirname "$gomod")" 1>&2
  echo "Processing $gomod" 1>&2

  go mod tidy 1>&2
  go mod download all 1>&2
  go list -m -json all | jq -c

  popd 1>&2
done