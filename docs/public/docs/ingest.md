---
description: Ingest
---
# Ingest

Quesma supports the standard Elastic/OpenSearch API endpoints for ingest. Similar to Elastic/OpenSearch, the ingest process is very flexible, allowing you to ingest data without manually specifying the schema or without having to create Elastic index or ClickHouse tables prior to inserting data. Quesma can also act as a transparent proxy, allowing you to ingest data into your existing Elastic/OpenSearch cluster.

Assuming a fresh installation of latest Quesma, follow these steps to connect the ingest data to Quesma and ClickHouse:

1. Pick the Elastic index that is the destination in the incoming data (see [Ingesting data through Quesma](#ingesting-data-through-quesma-using-elasticopensearch-compatible-endpoints))
2. Enable and configure it in Quesma's configuration (see [Ingesting data through Quesma](#ingesting-data-through-quesma-using-elasticopensearch-compatible-endpoints))
3. Customize the schema (recommended) via:
    - Sending the mapping manually to Quesma's explicit mapping endpoint (see [Explicit mappings](#explicit-mappings-recommended))
    - Mapping configuration in Quesma configuration file (see [Explicit mappings](#explicit-mappings-recommended))
4. Let Quesma create the ClickHouse table based on the first record of data (see [Automatic schema inference](#automatic-schema-inference)) and optional schema configuration (see [Explicit mappings](#explicit-mappings-recommended))
5. Check the result in Routing and Schemas tabs of Quesma console or query the data in Kibana/OpenSearch Dashboards (see [Ingest observability](#ingest-observability))

## Ingesting data through Quesma using Elastic/OpenSearch compatible endpoints

First, pick up your first Elasticsearch index that is defined in the data you are about to ingest and make sure that you have enabled the index in the Quesma's ingest processor configuration:

```yaml
processors:
  - name: my-ingest-processor
    type: quesma-v1-processor-ingest
    config:
      indexes:
        my_index:
          target:
            - backend-clickhouse
        "*": # Always required
          target:
            - backend-elasticsearch
```

This way, Quesma will know that the data of that index should be ingested to ClickHouse.

Quesma supports the following Elastic/OpenSearch ingest endpoints:
* `POST /_bulk`
* `POST /:index/_bulk`
* `POST /:index/_doc`

This means that your existing ingest infrastructure to Elastic/OpenSearch will work exactly the same with Quesma. The only change required will be to point them to the Quesma endpoint instead of Elastic/OpenSearch.

Quesma works correctly with:
* [Logstash](https://www.elastic.co/logstash)
* [Filebeat](https://www.elastic.co/beats/filebeat)
* [Elastic Agent](https://www.elastic.co/elastic-agent)
* [ElasticSearch Sink Connector (for Kafka)](https://docs.confluent.io/kafka-connectors/elasticsearch/current/overview.html)

[Ingest pipelines API endpoint](https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest.html) is currently **NOT** supported.

### Optional: ingesting data directly into ClickHouse

Apart from using Quesma ingest capabilities, you can also insert data directly into ClickHouse, without going through Quesma. The data for an enabled index is stored in a ClickHouse table with the same name as the index. All data modifications (via `INSERT`/`UPDATE`/`DELETE`/etc.) will be reflected in results of queries made through Quesma.

## Schema management

Quesma is flexible in terms of schema in similar ways to Elastic/OpenSearch. For example, you can start ingestion without specifying the schema in advance or without creating the ClickHouse table beforehand. Quesma will automatically create the ClickHouse table based on the data it receives, inferring the schema (column types) from the data. In some cases, you may want to manually specify or tweak the schema, for which Quesma provides ways to do so.

### Automatic schema inference

Similarly to Elastic/OpenSearch ([dynamic mapping](https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-mapping.html)), Quesma will try to infer data types based on the data it receives.

When you ingest the first document without having created the ClickHouse table manually, Quesma will issue a `CREATE TABLE` based on the infered schema of the first document.

Note that this puts certain requirement on first record of data ingested (similarly to Elastic/OpenSearch) as it will define the schema for your data. If plan to use this approach, make sure the first ingested record is a good representation of the data you plan to ingest. Pay attention to avoid missing fields, nulls and zeros.

If any later documents contain new fields, Quesma will automatically add these fields to the ClickHouse table via an `ALTER TABLE` statement (see [Adding new fields](#schema-evolution-adding-new-fields)).

### Explicit mappings (recommended)

Apart from automatic schema inference, you can also specify the schema explicitly, before sending first bytes of data. You can use the explicit mapping functionality by [sending the mapping manually](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html) to Quesma (see the [linked Elastic guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html)). The received mapping takes precedence over the inferred schema from the data and will be used by Quesma when creating the ClickHouse table or when querying the index.

You can also specify the explicit mappings in the Quesma configuration file with the `schemaOverrides` option:

```yaml
my_index:
  target:
    - backend-clickhouse
  schemaOverrides:
    fields:
      "products.manufacturer":
        type: "text"
      "geoip.location":
        type: "geo_point"
```

This can be useful if you are unable to send the mapping to the mapping endpoint or some integration is sending some invalid mapping (see [Ingest observability](#ingest-observability) to troubleshoot issues with schema).

### Schema configuration priority

When ingesting data, Quesma will incorporate the schema information from both the automatic schema inference and explicit mappings. The priority is as follows (from highest to lowest): 

1. Explicit mapping in the Quesma configuration file
2. Explicit mapping sent to the mapping endpoint (`PUT /:index` or `PUT /:index/_mapping`)
3. Inferred schema from the data

If there is some conflict between the inferred schema and the explicit mapping, the explicit mapping will take precedence (with the mapping in the configuration file taking the precedence).

### Physical storage 

Quesma stores each index in a separate database table by default for convenience and performance reasons.

However, you can also store multiple indexes in a single table. To do this, configure each index as shown in the following example:
```yaml
indexes:
  first_index:
    target:
      - backend-clickhouse:
          useCommonTable: true
  second_index:
    target:
      - backend-clickhouse:
          useCommonTable: true
  "*":
    target:
      - backend-elastic
  ...        
```

These indexes will then be stored in the `quesma_common_table` table.

### Schema evolution: adding new fields

When new fields are added to the data sent to Quesma, Quesma will automatically add these fields to the ClickHouse table via an `ALTER TABLE` statement.

If you wish to customize the field type or other properties of the new field, you can do so by sending an updated mapping to the mapping endpoint or by updating the explicit mapping in the Quesma configuration file.

## Scalability

### Horizontal Scaling for Ingestion

Quesma supports horizontal scaling for data ingestion, allowing you to start multiple instances of Quesma to handle higher ingestion loads. This means you can distribute the ingestion workload across several Quesma instances, improving the overall throughput and reliability of the data ingestion process.

To horizontally scale Quesma for ingestion:
1. Deploy multiple Quesma instances, each configured to handle a portion of the ingestion workload.
2. Use a load balancer to distribute incoming data ingestion requests across the available Quesma instances.

This setup ensures that the ingestion process can handle large volumes of data efficiently by leveraging the combined processing power of multiple Quesma instances.

::: warning
Note that while Quesma supports horizontal scaling for ingestion, it is important to note that querying **must** be handled by a single Quesma instance.
:::

## Ingest observability

The Quesma debugging interface (by default available at `http://localhost:9999`) provides helpful statistics related to the ingest process.

The "Dashboard" tab provides a real-time view of the ingest process, showing the number of requests sent to ClickHouse and Elastic/OpenSearch. The "Routing" tab can be used to determine if the ingest request was correctly routed.

The "Schemas" tab shows the schema of indexes and can be used to see the schema inferred by Quesma or the schema specified explicitly.