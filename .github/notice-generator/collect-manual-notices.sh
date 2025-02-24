#!/bin/bash -e

# Collect manually created notices we might have in the repository
# (for example if we use code from a project, but that project isn't
# a Go dependency of our project)

for notice in **/NOTICE.MD ; do
  if [ -f "$notice" ]; then
    echo "" # newline between added notices
    cat "$notice"
  fi
done