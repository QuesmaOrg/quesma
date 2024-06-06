# Library for common functions and setup

DEFAULT_COMPOSE_FILE="docker/local-dev.yml"
: "${QUESMA_COMPOSE_FILE:=$DEFAULT_COMPOSE_FILE}"
if [ ! -z "$1" ]; then
  QUESMA_COMPOSE_FILE="$1" # it can be file na,e
  if [ ! -f "$QUESMA_COMPOSE_FILE" ]; then # orname
    QUESMA_COMPOSE_FILE="docker/$1.yml"
  fi
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

# Get the current git commit hash
QUESMA_BUILD_DATE=$(git --no-pager log -1 --date=format:'%Y-%m-%d' --format="%ad")
QUESMA_VERSION=$(git describe)
QUESMA_BUILD_SHA=$(git rev-parse --short HEAD)




