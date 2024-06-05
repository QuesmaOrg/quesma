#!/bin/bash
# Builds and start the local development environment
set -e
cd "$(dirname "$0/")/.."
source bin/lib.sh

docker compose  -f "$QUESMA_COMPOSE_FILE" build --build-arg QUESMA_BUILD_DATE="$QUESMA_BUILD_DATE" --build-arg QUESMA_VERSION="$QUESMA_VERSION" --build-arg QUESMA_BUILD_SHA="$QUESMA_BUILD_SHA" && docker compose -f "$QUESMA_COMPOSE_FILE" up -d

cat <<"EOF"
               ________
               \_____  \  __ __   ____   ______ _____ _____
                /  / \  \|  |  \_/ __ \ /  ___//     \\__  \
               /   \_/.  \  |  /\  ___/ \___ \|  Y Y  \/ __ \_
               \_____\ \_/____/  \___  >____  >__|_|  (____  /
                      \__>           \/     \/      \/     \/
EOF
echo "http://localhost:9999"