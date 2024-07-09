#!/bin/bash
# Adds sample data to Kibana
cd "$(dirname "$0")"
source ../lib.sh

wait_until_available

echo -n "Adding data view Kibana Sample Data eCommerce... "
do_silent_http_post "api/data_views/data_view" '{
    "data_view": {
       "name": "Kibana Sample Data eCommerce",
       "title": "kibana_sample_data_ecommerce",
       "id": "kibana_sample_data_ecommerce",
       "timeFieldName": "order_date",
       "allowNoIndex": true
    },
    "override": true
}'
echo ""

echo -n "Adding data view Kibana Sample Data Logs... "
do_silent_http_post "api/data_views/data_view" '{
    "data_view": {
       "name": "Kibana Sample Data Logs",
       "title": "kibana_sample_data_logs",
       "id": "kibana_sample_data_logs",
       "timeFieldName": "timestamp",
       "allowNoIndex": true
    },
    "override": true
}'
echo ""

echo -n "Adding data view Kibana Sample Data Flights... "
do_silent_http_post "api/data_views/data_view" '{
    "data_view": {
       "name": "Kibana Sample Data Flights",
       "title": "kibana_sample_data_flights",
       "id": "kibana_sample_data_flights",
       "timeFieldName": "timestamp",
       "allowNoIndex": true
    },
    "override": true
}'
echo ""


echo ""


echo -e "\nData views added."

