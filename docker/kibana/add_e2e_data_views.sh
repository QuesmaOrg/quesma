#!/bin/bash
# Adds sample data to Kibana
cd "$(dirname "$0")"
source lib.sh

wait_until_available

add_view() {
  local view_name=$1

  echo -n "Adding data view $view_name"
  do_silent_http_post "api/data_views/data_view" '{
      "data_view": {
         "name": "$view_name",
         "title": "$view_name",
         "id": "$view_name",
         "timeFieldName": "timestamp",
         "allowNoIndex": true
      },
      "override": true
}'



echo ""
echo ""
echo -e "\nData views added."