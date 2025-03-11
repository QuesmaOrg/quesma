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

echo -n "Adding alias for Kibana sample data - logs... "
curl --no-progress-meter -X POST "http://mitmproxy:8080/_aliases" \
    -H 'Content-Type: application/json' \
    -d '{
    "actions": [
      {
        "add": {
          "index": "kibana_sample_data_logs",
          "alias": "alias_kibana_sample_data_logs"
        }
      }
    ]
}'

echo ""

echo -n "Adding data view Elasticsearch: Kibana Sample Data - Logs... "
do_silent_http_post "api/data_views/data_view" '{
    "data_view": {
       "name": "Elasticsearch: Kibana Sample Data Logs",
       "title": "alias_kibana_sample_data_logs",
       "id": "alias-kibana-sample-data-logs",
       "timeFieldName": "timestamp",
       "allowNoIndex": true
    },
    "override": true
}'

echo -n "Adding data view Phone Home Data... "
do_silent_http_post "api/data_views/data_view" '{
    "data_view": {
       "name": "Quesma Phone Home Data",
       "title": "phone_home_data",
       "id": "phone_home_data",
       "timeFieldName": "@timestamp",
       "allowNoIndex": true
    },
    "override": true
}'


echo -n "Adding data view Windows Logs... "
do_silent_http_post "api/data_views/data_view" '{
    "data_view": {
       "name": "Windows Security Logs",
       "title": "windows_logs",
       "id": "windows_logs",
       "timeFieldName": "@timestamp",
       "allowNoIndex": true
    },
    "override": true
}'

echo -n "Adding data view Github Events Logs... "
do_silent_http_post "api/data_views/data_view" '{
    "data_view": {
       "name": "GitHub Events",
       "title": "github_events",
       "id": "github_events",
       "timeFieldName": "@timestamp",
       "allowNoIndex": true
    },
    "override": true
}'

echo -n "Adding data view Type Logs... "
do_silent_http_post "api/data_views/data_view" '{
    "data_view": {
       "name": "Type Logs",
       "title": "type_logs",
       "id": "type_logs",
       "timeFieldName": "@timestamp",
       "allowNoIndex": true
    },
    "override": true
}'

echo ""

echo ""


echo -e "\nData views added."

