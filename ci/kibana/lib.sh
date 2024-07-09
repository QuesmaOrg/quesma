#!/bin/bash
echo "$@"

DASHBOARD_URL="$1"
if [ -z "$DASHBOARD_URL" ]; then
  DASHBOARD_URL="http://kibana:5601"
fi

echo "$DASHBOARD_URL"

if [ -z "$XSRF_HEADER" ]; then
  XSRF_HEADER="kbn-xsrf: true"
fi

if [ -n "$ELASTICSEARCH_USER" ]; then
  echo "Using Basic Authentication"
  MAYBE_AUTH="-u $ELASTICSEARCH_USER:$ELASTICSEARCH_PASSWORD"
fi

wait_until_available() {
  local http_code

  echo "Waiting until '$DASHBOARD_URL' is available..."
  while [ "$http_code" != "200" ]; do
    http_code=$(curl --no-progress-meter -k -s -w "%{http_code}" -XGET "$DASHBOARD_URL/api/status" -o /dev/null )
    echo "HTTP Status Code: $http_code"

    if [ "$http_code" != "200" ]; then
        echo "Retrying in a second..."
        sleep 1
    fi
  done

  echo "'$DASHBOARD_URL' is available"
}

do_http_post() {
  local url=$1
  local body=$2

  curl --no-progress-meter -k ${MAYBE_AUTH} -X POST "$DASHBOARD_URL/$url" \
    -H "$XSRF_HEADER" \
    -H 'Content-Type: application/json' \
    -d "$body"
}

do_silent_http_post() {
  local url=$1
  local body=$2

  curl -w "HTTP %{http_code}" -k -o /dev/null --no-progress-meter ${MAYBE_AUTH} -X POST "$DASHBOARD_URL/$url" \
    -H "$XSRF_HEADER" \
    -H 'Content-Type: application/json' \
    -d "$body"
}

add_sample_dataset() {
    local sample_data=$1
    START_TIME=$(date +%s)
    echo "Adding $sample_data dataset"
    do_http_post "api/sample_data/$sample_data" ''
    END_TIME=$(date +%s)
    echo -e "\nAdded $sample_data dataset, took $((END_TIME-START_TIME)) seconds"
}
