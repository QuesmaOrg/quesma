# Minimalistic ELK-CH configuration

Kibana is available at [localhost:5601](http://localhost:5601/app/observability-log-explorer/).

### MITM Proxy
Mitmweb is available at [localhost:8081](http://localhost:8081).

It is a man in the middle inspection tool, please [consult the mitmproxy documentation](https://docs.mitmproxy.org/stable/).

You can add your own Python script in `mitmproxy/request.py`. Please consult above documentation or ChatGPT for results.

You can see sample ElasticSearch queries in `mitmproxy/requests/`. For example
```bash
tail -f mitmproxy/requests/logs-X-X.txt
```

### Clickhouse-client
To connect to the client when `clickhouse-server` is running on [localhost:8123/play](http://localhost:8123/play)

Alternatively you can find container name using `docker ps` and use command line:

```bash
docker exec -it poc-elk-mitmproxy-clickhouse-1 clickhouse-client
```

Once you connected you run typical SQL commands such as:
```sql
SHOW TABLES;
DESCRIBE logs;
SELECT * FROM logs LIMIT 10;
```
