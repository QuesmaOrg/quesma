#!/bin/bash

while [ "$http_code" != "200" ]; do
    http_code=$(curl --no-progress-meter -k -s -w "%{http_code}" -XGET http://kibana:5601/api/status -o /dev/null )
    echo "HTTP Status Code: $http_code"

    if [ "$http_code" != "200" ]; then
        echo "Retrying in a second..."
        sleep 1
    fi
done


# Kibana just requires `kbn-xsrf` header presence, any value can be used
echo "Adding logs dataset"
curl --no-progress-meter -XPOST -H "kbn-xsrf: arbitrary-header" http://kibana:5601/api/sample_data/logs
echo -e "\nAdding ecommerce dataset"
curl --no-progress-meter -XPOST -H "kbn-xsrf: arbitrary-header" http://kibana:5601/api/sample_data/ecommerce
echo -e "\nAdding flights dataset"
curl --no-progress-meter -XPOST -H "kbn-xsrf: arbitrary-header"  http://kibana:5601/api/sample_data/flights

echo -e "\nSample datasets added."

echo -e "\nAdding data views for our test data"

curl --no-progress-meter -XPOST \
-H 'Content-Type: application/json' \
-H "kbn-xsrf: arbitrary-header" \
http://kibana:5601/api/data_views/data_view -d '{
    "data_view": {
       "name": "Device Logs W/O Timestamp",
       "title": "device*",
       "id": "device-logs-no-timestamp",
       "allowNoIndex": true
    },
    "override": true
}'
echo ""
curl --no-progress-meter -XPOST \
-H 'Content-Type: application/json' \
-H "kbn-xsrf: arbitrary-header" \
http://kibana:5601/api/data_views/data_view -d '{
    "data_view": {
       "name": "Device Logs",
       "title": "device*",
       "id": "device-logs-elasticsearch-timestamp",
       "timeFieldName": "epoch_time",
       "allowNoIndex": true
    },
    "override": true
}'

echo ""
curl --no-progress-meter -XPOST \
-H 'Content-Type: application/json' \
-H "kbn-xsrf: arbitrary-header" \
http://kibana:5601/api/data_views/data_view -d '{
    "data_view": {
       "name": "Our Generated Logs",
       "title": "logs-generic-*",
       "id": "logs-generic",
       "timeFieldName": "@timestamp",
       "allowNoIndex": true
    },
    "override": true
}'

echo -e "\nData views added."
