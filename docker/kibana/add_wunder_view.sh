#!/bin/bash
# Adds a data view to Kibana
cd "$(dirname "$0")"
source lib.sh

wait_until_available

echo -n "Adding data view Wunder... "
do_silent_http_post "api/data_views/data_view" '{
  "data_view": {
    "title": "211318",
    "name": "Wunder",
    "timeFieldName": "timestamp",
    "allowNoIndex": true
  }
}'
