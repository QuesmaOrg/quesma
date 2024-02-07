# MVP of Quesma using ELK stack with Clickhouse

Minimal Viable Product of Quesma. It provides Elasticsearch API compatibility for the observability use case
with Clickhouse as a backend.

The main debugging interface is available at [localhost:9999](http://localhost:9999). You can follow the links.

## Architecture

### Quesma

#### L4 Proxy

```mermaid
flowchart LR;
    client-->l4_proxy;
    l4_proxy-->client;
    l4_proxy-->elasticsearch;
    elasticsearch-->l4_proxy;
    l4_proxy-.->http_server;
    http_server-.->statistics_processor;
```

#### Dual-Write

```mermaid
flowchart LR;
    client-->routing_proxy;
    routing_proxy-->elasticsearch;
    elasticsearch-->routing_proxy;
    routing_proxy-->http_server;
    http_server-.->statistics_processor;
    http_server-->clickhouse;
    http_server-->routing_proxy;
    clickhouse-->http_server;
    http_server-->response_matcher;
    routing_proxy-->response_matcher;
    routing_proxy-->client;
    response_matcher-.->UI((UI))
```

### Docker Compose Setup

```mermaid
flowchart LR;
    kibana-->mitmproxy;
    mitmproxy-->kibana;
    device-log-generator-->mitmproxy;
    query-generator-->mitmproxy;
    log-generator-->mitmproxy;
    mitmproxy-->quesma;
    quesma-->mitmproxy;
    quesma-->clickhouse[(clickhouse)];
    clickhouse-->quesma;
    quesma-->elasticsearch[(elasticsearch)];
    elasticsearch-->quesma;
```

### Kibana

Kibana is available at [localhost:5601](http://localhost:5601/app/observability-log-explorer/).

### MITM Proxy
Mitmweb is available at [localhost:8081](http://localhost:8081).

It is a man in the middle inspection tool, please [consult the mitmproxy documentation](https://docs.mitmproxy.org/stable/).

You can enable the Python script by uncommenting `docker-compose.yml` in `services.mitmproxy.run`.

You can further edit it `mitmproxy/request.py`.

Very useful for quick dumps in `mitmproxy/requests`:
```bash
tail -f mitmproxy/requests/logs-X-X.txt
```

Some filters that you might find useful for filtering out noise requests (copy-paste into the `Search` box):
```bash

!/_doc & !security & !metrics & !.kibana_alerting & !_nodes &!kibana_task_manager & !_pit & !_monitoring & !_xpack & !.reporting & !.kibana & !heartbeat & !_aliases & !_field_caps & !_license & !.logs-endpoint & !.fleet- & !traces & !_cluster & !_resolve & !_mapping & !logs-cloud & !.monitoring & !.ds-risk
```
This will also filter out insert requests:
```bash
!/_doc & !security & !metrics & !.kibana_alerting & !_nodes &!kibana_task_manager & !_pit & !_monitoring & !_xpack & !.reporting & !.kibana & !heartbeat & !_aliases & !_field_caps & !_license & !.logs-endpoint & !.fleet- & !traces & !_cluster & !_resolve & !_mapping & !logs-cloud & !.monitoring & !.ds-risk & !_bulk
```

### Clickhouse-client
To connect to the client when `clickhouse-server` is running on [localhost:8123/play](http://localhost:8123/play)

Alternatively, you can find the container name using `docker ps` and use the command line:

```bash
docker exec -it poc-elk-mitmproxy-clickhouse-1 clickhouse-client
```

Once you connected, you run typical SQL commands such as:
```sql
SHOW TABLES;
DESCRIBE logs;
SELECT * FROM logs LIMIT 10;
```
