#!/bin/bash
# Adds a data view to Kibana
set -e

curl -X POST "kibana:5601/api/data_views/data_view" -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d'
{
  "data_view": {
    "title": "211318",
    "name": "Wunder",
    "timeFieldName": "timestamp",
    "allowNoIndex": true
  }
}'