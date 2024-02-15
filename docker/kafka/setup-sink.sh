#!/bin/bash

while [ "$http_code" != "200" ]; do
    http_code=$(curl -k -s -w "%{http_code}" -XGET http://kafka-connect:8083/connectors -o /dev/null )
    echo "HTTP Status Code: $http_code"

    if [ "$http_code" != "200" ]; then
        echo "Retrying in a second..."
        sleep 1
    fi
done


# Create a sink
curl -X POST http://kafka-connect:8083/connectors -H 'Content-Type: application/json' -d \
'{
  "name": "elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics": "kafka-example-topic",
    "key.ignore": "true",
    "schema.ignore": "true",
    "connection.url": "http://mitmproxy:8080",
    "type.name": "_doc",
    "name": "elasticsearch-sink",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}'


### SOME USEFUL KAFKA KNOWLEDGE
### UPDATE SINK
#curl -X PUT http://localhost:8083/connectors/elasticsearch-sink/config -H 'Content-Type: application/json' -d \
#'{
#    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
#    "tasks.max": "1",
#    "topics": "example-topic",
#    "key.ignore": "true",
#    "schema.ignore": "true",
#    "connection.url": "http://mitmproxy:8080",
#    "type.name": "_doc",
#    "name": "elasticsearch-sink",
#    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
#    "value.converter.schemas.enable": "false"
#}'
