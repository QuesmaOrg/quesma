# Transparent Elasticsearch proxy

In this scenario, Quesma is used as a transparent proxy between Kibana/OpenSearchDashboards(OSD) and Elasticsearch/OpenSearch. 

```mermaid
flowchart LR 
    K[Kibana/OSD] -.-o|Ingest| Q((Quesma)) -.-o|Ingest| E[(Elasticsearch/OpenSearch)]
    K[Kibana/OSD] -->|Query| Q((Quesma)) -->|Query| E[(Elasticsearch/OpenSearch)]
```

### Quesma installation

**Prerequisites:** 
* Elasticsearch/OpenSearch is running.
* Kibana/OSD is running.
* [Docker is installed](https://www.docker.com/get-started/), at least 20.10 version.

**Installation steps:**

1. Create a configuration file named `quesma.yaml` with the following content, make sure to replace placeholders (`#PLACE_YOUR*`) with actual values.
    ```yaml
    frontendConnectors:
      - name: elastic-ingest
        type: elasticsearch-fe-ingest
        config:
          listenPort: 8080
      - name: elastic-query
        type: elasticsearch-fe-query
        config:
          listenPort: 8080
    backendConnectors:
      - name: backend-elasticsearch
        type: elasticsearch
        config:
          url: #PLACE_YOUR_ELASTICSEARCH_URL_HERE
          user: #PLACE_YOUR_ELASTICSEARCH_USERNAME_HERE
          password: #PLACE_YOUR_ELASTICSEARCH_PASSWORD_HERE
    processors:
      - name: my-noop-processor
        type: quesma-v1-processor-noop
    pipelines:
      - name: elasticsearch-proxy-read
        frontendConnectors: [ elastic-query ]
        processors: [ my-noop-processor ]
        backendConnectors: [ backend-elasticsearch ]
      - name: elasticsearch-proxy-write
        frontendConnectors: [ elastic-ingest ]
        processors: [ my-noop-processor ]
        backendConnectors: [ backend-elasticsearch ]
    ```
> Note: To learn more about configuration options, refer to [Configuration primer](/config-primer.md)

2. Run Quesma with the following command:
    ```bash
    docker run --name quesma -p 8080:8080 \
     -e QUESMA_CONFIG_FILE=/configuration/quesma.yaml \
    -v $(pwd)/quesma.yaml:/configuration/quesma.yaml quesma/quesma:latest 
    ```
    You now have a running Quesma instance running with Elasticsearch API endpoint on port 8080. You can also enable Quesma's admin panel at [localhost:9999](http://localhost:9999/) by adding `-p 9999:9999` to docker run command.
3. Reconfigure client endpoint
   * For Kibana: Update your [Kibana configuration](https://www.elastic.co/guide/en/kibana/current/settings.html), so that it points to Quesma Elasticsearch API endpoint mentioned above, instead of Elasticsearch original endpoint. In your Kibana configuration file, replace the `elasticsearch.hosts` value with Quesma's host and port, e.g.: 
      ```yaml
      elasticsearch.hosts: ["http://localhost:8080"]
      ```
     or optionally use `ELASTICSEARCH_HOSTS` environment variable.
   * For OpenSearchDashboards: modify [`opensearch_dashboards.yml` file](https://opensearch.org/docs/latest/install-and-configure/configuring-dashboards/) and change the `opensearch.hosts` property.
4. Restart Kibana/OSD.
