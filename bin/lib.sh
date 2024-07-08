# Library for common functions and setup

DEFAULT_COMPOSE_FILE="docker/local-dev.yml"
QUESMA_STATUS_FILE="bin/.running-docker-compose"

: "${QUESMA_COMPOSE_FILE:=$DEFAULT_COMPOSE_FILE}"
if [ ! -z "$1" ]; then
  QUESMA_COMPOSE_FILE="$1" # it can be file path
  if [ ! -f "$QUESMA_COMPOSE_FILE" ]; then # or name
    QUESMA_COMPOSE_FILE="docker/$1.yml"
  fi
  echo "QUESMA_COMPOSE_FILE set to '$QUESMA_COMPOSE_FILE' by argument '$1'"
else
  if [ "$QUESMA_COMPOSE_FILE" != "$DEFAULT_COMPOSE_FILE" ]; then
    echo "QUESMA_COMPOSE_FILE overridden by env to $QUESMA_COMPOSE_FILE"
  else
    if [ -f "$QUESMA_STATUS_FILE" ]; then
      QUESMA_COMPOSE_FILE="$(cat $QUESMA_STATUS_FILE)"
      echo "QUESMA_COMPOSE_FILE set to '$QUESMA_COMPOSE_FILE' from '$QUESMA_STATUS_FILE'"
    fi
  fi
fi

remove_status_file() {
  if [ -f "$QUESMA_STATUS_FILE" ]; then
    echo "Removing '$QUESMA_STATUS_FILE'"
    rm $QUESMA_STATUS_FILE
  fi
}

create_status_file() {
  echo "Saving QUESMA_COMPOSE_FILE='$QUESMA_COMPOSE_FILE' to status file '$QUESMA_STATUS_FILE'"
  echo "$QUESMA_COMPOSE_FILE" > "$QUESMA_STATUS_FILE"
}

if [ ! -f "$QUESMA_COMPOSE_FILE" ]; then
  echo "File '$QUESMA_COMPOSE_FILE' does not exist."
  remove_status_file
  exit 1
fi

# Get the current git commit hash
QUESMA_BUILD_DATE=$(git --no-pager log -1 --date=format:'%Y-%m-%d' --format="%ad")
QUESMA_VERSION=$(git describe)
QUESMA_BUILD_SHA=$(git rev-parse --short HEAD)




