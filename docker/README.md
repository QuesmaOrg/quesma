### Our docker container setup

This directory contains:
* docker-compose files for running Quesma and/or auxiliary services like Elasticsearch/Clickhouse/Kafka 
or our own services like log-generator.
* any configuration files/scripts, etc. that are mounted into the containers 

## Notes about specific docker-compose files


* `ci.yml` - used in our CI (GitHub actions), minimal set of services and data for PR checks
* `local-dev.yml` - used for local development (and demos) - contains running Quesma with almost all the services and data
* `local-debug.yml` - used for debugging Quesma when running from IDE, contains only auxiliary services which connect to the local process
* `kafka-demo.yml` - created specifically for Device demo, contains all services and data, including Kafka, which writes to Quesma via Elasticsearch Connector.

