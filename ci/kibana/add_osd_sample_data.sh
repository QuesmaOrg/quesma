#!/bin/bash
# Adds sample data to Kibana
cd "$(dirname "$0")"
DASHBOARD_URL="http://opensearch-dashboards:5601"
XSRF_HEADER="osd-xsrf: true"
source lib.sh

wait_until_available

add_sample_dataset "flights"
add_sample_dataset "logs"
add_sample_dataset "ecommerce"

echo -n "Adding index pattern logs-generic... "
do_http_post "api/saved_objects/index-pattern/logs-generic" '{
    "attributes": {
       "name": "Our Generated Logs",
       "title": "logs-generic-*",
       "timeFieldName": "@timestamp",
       "allowNoIndex": true
    }
}'
echo ""

echo -n "Adding index pattern device-logs... "
do_http_post "api/saved_objects/index-pattern/device-logs" '{
    "attributes": {
       "name": "Device Logs",
       "title": "device*",
       "timeFieldName": "epoch_time",
       "allowNoIndex": true
    }
}'
echo ""
