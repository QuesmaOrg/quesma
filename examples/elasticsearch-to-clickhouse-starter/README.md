Quesma "elasticsearch-to-clickhouse" starter
============================================

This is a very simplistic Quesma setup to get you started.
Quesma exposes Elasticsearch-compatible HTTP REST API at http://localhost:8080.
You can view your data though Kibana at http://localhost:5601.

docker-compose file located in this folder creates four containers: Quesma, Elasticsearch, ClickHouse and Kibana.

Everything is stored in ClickHouse. No sample data sets are being loaded.

To run this example, simply execute:
```shell
docker-compose up -d
```