#!/bin/bash

while true; do
    echo "Writing following [ $json ] to Kafka"
    user_id=$(shuf -i 1-100000 -n 1)  # Generates a random userId between 1 and 100000
    json="{\"userId\": \"$user_id\", \"action\": \"login\"}"
    echo "$json" | /usr/bin/kafka-console-producer --broker-list broker:9092 --topic kafka-example-topic
    sleep 1
done
