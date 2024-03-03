#!/bin/bash

ENDPOINT=${OPENSEARCH_DASHBOARD_HOST:-kibana:5601}
XSRF=${OPENSEARCH_XSRF:-kbn-xsrf}


while [ "$http_code" != "200" ]; do
    http_code=$(curl --no-progress-meter -k -s -w "%{http_code}" -XGET http://$ENDPOINT/api/status -o /dev/null )
    echo "HTTP Status Code: $http_code"

    if [ "$http_code" != "200" ]; then
        echo "Retrying in a second..."
        sleep 1
    fi
done

add_kibana_sample_dataset() {
    local sample_data=$1
    START_TIME=$(date +%s)
    echo "Adding $sample_data dataset"
    curl --no-progress-meter -XPOST -H "$XSRF: arbitrary-header" http://$ENDPOINT/api/sample_data/$sample_data
    END_TIME=$(date +%s)
    echo -e "\nAdded $sample_data dataset, took $((END_TIME-START_TIME)) seconds"
}

if [ -z "$LIMITED_DATASET" ] || [ "$LIMITED_DATASET" != "true" ]; then
    add_kibana_sample_dataset "flights"
    add_kibana_sample_dataset "logs"
    add_kibana_sample_dataset "ecommerce"
else
    echo "Using limited dataset - only 'flights' index"
    add_kibana_sample_dataset "flights"
fi


curl --silent -o /dev/null  --no-progress-meter -XPOST \
-H 'Content-Type: application/json' \
-H "$XSRF: arbitrary-header" \
http://$ENDPOINT/api/data_views/data_view -d '{
    "data_view": {
       "name": "Our Generated Logs",
       "title": "logs-generic-*",
       "id": "logs-generic",
       "timeFieldName": "@timestamp",
       "allowNoIndex": true
    },
    "override": true
}'

echo ""
curl --silent -o /dev/null  --no-progress-meter -XPOST \
-H 'Content-Type: application/json' \
-H "$XSRF: arbitrary-header" \
http://$ENDPOINT/api/data_views/data_view -d '{
    "data_view": {
       "name": "Device Logs W/O Timestamp",
       "title": "device*",
       "id": "device-logs-no-timestamp",
       "allowNoIndex": true
    },
    "override": true
}'
echo ""
curl --silent -o /dev/null  --no-progress-meter -XPOST \
-H 'Content-Type: application/json' \
-H "$XSRF: arbitrary-header" \
http://$ENDPOINT/api/data_views/data_view -d '{
    "data_view": {
       "name": "Device Logs",
       "title": "device*",
       "id": "device-logs-elasticsearch-timestamp",
       "timeFieldName": "epoch_time",
       "allowNoIndex": true
    },
    "override": true
}'
curl --silent -o /dev/null --no-progress-meter -XPOST \
-H 'Content-Type: application/json' \
-H "$XSRF: arbitrary-header" \
http://$ENDPOINT/api/data_views/data_view -d '{
    "data_view": {
       "name": "Quesma Logs",
       "title": "quesma-logs-*",
       "id": "quesma-logs-from-filebeat",
       "timeFieldName": "@timestamp",
       "allowNoIndex": true
    },
    "override": true
}'
echo ""

echo -e "\nData views added."

