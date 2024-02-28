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
* `hydrolix.yml` - to be used with Hydrolix, requires `.env` file from 1Password. You like want also to create a data view in Kibana, see below.

### Hydrolix data view creation
```bash
curl -X POST "localhost:5601/api/data_views/data_view" -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d'
{
  "data_view": {
    "title": "211318",
    "name": "Wunder",
    "timeFieldName": "timestamp",
    "allowNoIndex": true
  }
}
'
```