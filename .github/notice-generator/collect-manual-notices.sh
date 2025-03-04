#!/bin/bash -ex

# Collect manually created notices we might have in the repository
# (for example if we use code from a project, but that project isn't
# a Go dependency of our project)

find */ -type f -iname NOTICE.MD | while read -r notice; do
  echo "$notice" 1>&2

  echo "" # newline between added notices
  cat "$notice"
done