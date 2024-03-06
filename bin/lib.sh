# Library for common functions and setup

DEFAULT_COMPOSE_FILE="docker/local-dev.yml"
: "${QUESMA_COMPOSE_FILE:=$DEFAULT_COMPOSE_FILE}"
if [ ! -z "$1" ]; then
  QUESMA_COMPOSE_FILE="docker/$1.yml"
  echo "QUESMA_COMPOSE_FILE set to '$QUESMA_COMPOSE_FILE' by argument '$1'"
else
  if [ "$QUESMA_COMPOSE_FILE" != "$DEFAULT_COMPOSE_FILE" ]; then
    echo "QUESMA_COMPOSE_FILE overriden by env to $QUESMA_COMPOSE_FILE"
  fi
fi

if [ ! -f "$QUESMA_COMPOSE_FILE" ]; then
  echo "File '$QUESMA_COMPOSE_FILE' does not exist."
  exit 1
fi