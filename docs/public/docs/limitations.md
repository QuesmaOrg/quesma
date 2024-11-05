# Known limitations or unsupported functionalities

Quesma is designed for analytical text queries such as in observability or security.
It does literal case-insensitive matches.
It does not do tokenization, natural language processing or scoring.
* Unsupported example: You need top 10 results for query car and expect to find article with word automobile.
* Good fit: Searching for logs such as `InvalidPassword` should also match `InvalidPasswordError` without `*` as required in Elastic.

## Software and hardware requirements
We recommend setting up at least 2 CPU cores and 2GBs of RAM for the Quesma container. The requirements may vary depending on the workload and the number of concurrent connections.
Quesma has been tested with the following software versions:
| Software                         | Version         |
|----------------------------------|-----------------|
| Docker                           | `24.0.7`        | 
| Elasticsearch/Kibana             | `8.11.1`        |
| ClickHouse                       | `24.1`, `23.12` |
| ClickHouse Cloud                 | `24.5`          |
| OpenSearch/OpenSearch Dashboards | `2.12.0`        |
| Hydrolix                         | `v4.8.12`       |

### ClickHouse limitations
* When using a cluster deployment of ClickHouse, the tables automatically created by Quesma (during [Ingest](/ingest.md)) will use the `MergeTree` engine. If you wish to use the `ReplicatedMergeTree` engine instead, you will have to create the tables manually with  `ReplicatedMergeTree` engine before ingesting data to Quesma.
  * *Note: On ClickHouse Cloud, the tables automatically created by Quesma will use the `ReplicatedMergeTree` engine (ClickHouse Cloud default engine).* 

## Functional limitations
Currently supported:
- front-end support for Kibana and Open Search Dashboards, limited to Discover(LogExplorer) interface and Dashboard panels
- read-only back-end support for Elastic/OpenSearch as origin source and ClickHouse or Hydrolix as destination source
- most popular [Query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html),
  including: `boolean`, `match`, `match phrase`, `multi-match`, `query string`, `nested`, `match all`, `exists`, `prefix`, `range`, `term`, `terms`, `wildcard`
- most popular [Aggregations](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html),
  including: `avg`, `cardinality`, `max`, `min`, `percentile ranks`, `percentiles`, `stats`, `sum`, `top hits`, `top metrics`, `value counts`,
  `date histogram`, `date range`, `filter`, `filters`, `histogram`, `range`, `singificant terms`, `terms`

Which as a result allows you to run Kibana/OSD queries and dashboards on data residing in ClickHouse/Hydrolix.


Will be relaxed, but not considered as best practice:
* Quesma does not allow mixed-data source queries, e.g. calling `GET /data_a,data_b/_search` where `data_a` is in Elasticsearch and `data_b` is ClickHouse table.
  * For Kibana user, this means that Data View cannot contain multiple indices backed up by different data sources.
  * For Elasticsearch API user, this means that you cannot perform queries like `GET /data_a,data_b/_search`, where `data_a` is in Elasticsearch and `data_b` is ClickHouse table.
* Management API is not supported.

Currently not supported future roadmap items:
* Some Query DSL features.
* Some aggregations, esp. those operating on `geo_point`/`geo_shape` types
* Quesma does not support all Elasticsearch API endpoints. Please
  refer to the `List of supported endpoints` section for more details.
* JSON are not pretty printed in the response.
* The schema support is limited.
  * Elasticsearch types: date, text, keyword, boolean, byte, short, integer, long, unsigned_long, float, double
  * Clickhouse types: DateTime, DateTime64, String, Boolean, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64
* Some advanced query parameters are ignored.
* No support for SQL, EQL (Event Query Language), PPL, or ES/QL.
* Better secret support.



## Performance limitations
* A single Quesma container can process 50 concurrent HTTP requests. More requests would receive an HTTP 429 status code.
* Async results are stored for 15 minutes. Only 10k or 500MB of async results are supported. They are not persisted across restarts.
* No more than 10,000 result hits.
* No partial results for long-running queries. All results are returned in one response once full query is finished
* No efficient support for metrics.


## List of supported endpoints

Quesma supports a subset of OpenSearch/Elasticsearch API endpoints.
Upon a query, Quesma will forward the request to the appropriate data source (Elasticsearch or ClickHouse).
The following endpoints are supported:

* Search:
  * `POST /_search`
  * `POST /:index/_search`
  * `POST /:index/_async_search`
  * `GET  /:index/_count`
* Schema:
  * `GET  /:index`
  * `GET  /:index/_mapping`
  * `POST /:index/_field_caps`
  * `POST /:index/_terms_enum`
  * `POST /:index`
  * `POST /:index/_mapping`
* Ingest:
  * `POST /_bulk`
  * `POST /:index/_bulk`
  * `POST /:index/_doc`
* Administrative:
  * `GET  /_cluster/health`


**Warning:** Quesma does not support path parameters in URLs listed above.

## List of supported Kibana features

Quesma allows querying data from Kibana or OpenSearch Dashboard, but not all features are supported.

For querying data, users can use:
* Discover, ref: [Kibana docs](https://www.elastic.co/guide/en/kibana/8.11/discover.html)
* Dashboards, ref: [Kibana docs](https://www.elastic.co/guide/en/kibana/8.11/dashboard.html)

Additional features:
* Alerting (ref: [Kibana docs](https://www.elastic.co/guide/en/kibana/8.11/kibana-alerts.html)) - limited to `Elasticsearch query` (`KQL or Lucene` and `Query DSL`) types
