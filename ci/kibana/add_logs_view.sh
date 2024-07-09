#!/bin/bash
# Adds a data view to Kibana
cd "$(dirname "$0")"
source lib.sh

wait_until_available

echo -n "Adding data view logs... "
do_silent_http_post "api/data_views/data_view" '{
  "data_view": {
    "title": "logs",
    "name": "logs",
    "timeFieldName": "reqTimeSec",
    "allowNoIndex": true
  }
}'
echo ""

echo -n "Adding data view logs... "
do_silent_http_post "api/data_views/data_view" '{
  "data_view": {
    "title": "siem",
    "name": "siem",
    "timeFieldName": "timestamp",
    "allowNoIndex": true
  }
}'
echo ""

