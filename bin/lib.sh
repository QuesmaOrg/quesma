# Library for common functions and setup

DEFAULT_COMPOSE_FILE="docker/local-dev.yml"
: "${QUESMA_COMPOSE_FILE:=$DEFAULT_COMPOSE_FILE}"
if [ "$QUESMA_COMPOSE_FILE" != "$DEFAULT_COMPOSE_FILE" ]; then
  echo "QUESMA_COMPOSE_FILE overriden to $QUESMA_COMPOSE_FILE"
fi

if [ ! -f "$QUESMA_COMPOSE_FILE" ]; then
  echo "File $QUESMA_COMPOSE_FILE does not exist."
  exit 1
fi