#!/bin/bash
# Builds and start the local development environment
set -e
cd "$(dirname "$0/")/.."

docker compose -f docker/local-dev.yml build && docker compose -f docker/local-dev.yml up -d

cat <<"EOF"
               ________
               \_____  \  __ __   ____   ______ _____ _____
                /  / \  \|  |  \_/ __ \ /  ___//     \\__  \
               /   \_/.  \  |  /\  ___/ \___ \|  Y Y  \/ __ \_
               \_____\ \_/____/  \___  >____  >__|_|  (____  /
                      \__>           \/     \/      \/     \/
EOF
echo "http://localhost:9999"