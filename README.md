# Minimalistic ELK-CH configuration

Kibana is available at [localhost:5061](http://localhost:5061)

### MITM Proxy
Mitmweb is available at [localhost:8081](http://localhost:8081).

It is a man in the middle inspection tool, please [consult the mitmproxy documentation](https://docs.mitmproxy.org/stable/).

### Clickhouse-client
To connect to the client when `clickhouse-server` is running, run <br>
```bash
docker exec -it clickhouse clickhouse-client
```
Alternatively you can visit `http://localhost:8123`
