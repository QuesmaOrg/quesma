---
description: Installation guide
---
# Installation guide

The installation guide provides detailed instructions on how to set up Quesma in various scenarios. Depending on your current setup and requirements, you can follow one of the scenarios:

1. [**Scenario I: Transparent Elasticsearch/OpenSearch proxy**](./example-1.md), where Quesma is used as a transparent proxy between Kibana and Elasticsearch. Start with this scenario, when:
   * You have existing Elasticsearch/OpenSearch with Kibana/OpenSearchDashboards as well as data sources like Logstash/Filebeat connected to them
   * You want to prove non-intrusiveness of Quesma by just placing it in-line and making sure everything works as before
2. [**Scenario II: Adding ClickHouse/Hydrolix tables to existing Kibana-Elasticsearch/OpenSearch ecosystem**](./example-2-1.md), where user already has data in Elasticsearch indices and wants to add ClickHouse/Hydrolix tables so that these are also visible and available for querying via Kibana/OSD. Follow this scenario, when:
   * You are upgrading from Scenario I by adding ClickHouse or Hydrolix as alternative storage
   * You want to keep reading and/or writing to some Elasticsearch/OpenSearch indexes along with reading and/or writing to ClickHouse/Hydrolix
3. [**Scenario III: Query ClickHouse/Hydrolix tables as Elasticsearch indices only**](./example-2-0.md), where Quesma is configured to expose ClickHouse/Hydrolix tables via Elasticsearch/OpenSearch APIs, making them visible in Kibana/OSDashboards and there's no need to ingest/query any data from Elasticsearch/OpenSearch. \
   Follow this scenario, when:
   * You don't want to have any remaining data in Elasticsearch/OpenSearch
   * You plan to query and/or ingest to ClickHouse or query Hydrolix data sources only
   * You are upgrading from Scenario II by migrating all data to ClickHouse/Hydrolix

Each scenario provides an example Quesma configuration for the given setup
