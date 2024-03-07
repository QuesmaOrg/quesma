#!/bin/bash
# Adds sample data to Kibana
cd "$(dirname "$0")"
source lib.sh

wait_until_available

if [ -z "$LIMITED_DATASET" ] || [ "$LIMITED_DATASET" != "true" ]; then
    add_sample_dataset "flights"
    add_sample_dataset "logs"
    add_sample_dataset "ecommerce"
else
    echo "Using limited dataset - only 'flights' index"
    add_sample_dataset "flights"
fi

echo -n "Adding data view logs-generic... "
do_silent_http_post "api/data_views/data_view" '{
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


echo -n "Adding data view Device W/O Timestamp... "
do_silent_http_post "api/data_views/data_view" '{
    "data_view": {
       "name": "Device Logs W/O Timestamp",
       "title": "device*",
       "id": "device-logs-no-timestamp",
       "allowNoIndex": true
    },
    "override": true
}'
echo ""

echo -n "Adding data view Device Logs... "
do_silent_http_post "api/data_views/data_view" '{
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

echo -n "Adding data view Quesma Logs... "
do_silent_http_post "api/data_views/data_view" '{
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

