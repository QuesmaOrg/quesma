#!/bin/bash
set -eu

dataset=("kibana_sample_data_ecommerce" "kibana_sample_data_flights" "kibana_sample_data_logs")

echo "Downloading data..."

for name in "${dataset[@]}"; do
  echo -n "  Downloading $name..."
  wget -q "https://storage.googleapis.com/elastic-sample-data/version-2024-04-22/$name.json"
  echo "  done"
done


echo "Loading it to $ELASTICSEARCH_URL..."

for name in "${dataset[@]}"; do
  echo "  Loading $name..."
  elasticdump --input="$name.json" --output="$ELASTICSEARCH_URL/$name" --type=data --limit=1000
done

echo "All done"