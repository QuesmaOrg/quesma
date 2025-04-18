---
description: Adding ClickHouse/Hydrolix tables to existing Kibana/Elasticsearch ecosystem
---
# Adding Hydrolix tables to existing Kibana/Elasticsearch ecosystem

In this scenario, user already has data in Elasticsearch/OpenSearch indices - let's assume `index1`, `index2` and `index3`.
Additionally, they have a Hydrolix instance with tables named `siem` and `logs`.

Quesma is configured to write and read from/to all Elasticsearch indices and, additionally, expose both Hydrolix tables via its Elasticsearch API, making them visible in Kibana/OpenSearch Dashboards (OSD) as Elasticsearch indices and available for query.

::: info Note
Ingest to Hydrolix is currently not supported, [let us know](https://quesma.com/contact) if you are interested in this functionality.
:::

```mermaid
flowchart LR
    Q --> |query: `siem`, `logs` | CH[(Hydrolix)]
    Q --> |query: `index1`, `index2`, `index3`
            ingest: `index1`, `index2`, `index3` | E[(Elasticsearch/OpenSearch)] 
    K[Kibana/OSD] --> Q((Quesma)) 
```

### Quesma installation

**Prerequisites:**
* Hydrolix is running.
* Kibana/OSD and Elasticsearch/OpenSearch are running.
* [Docker is installed](https://www.docker.com/get-started/), at least 20.10 version.

**Installation steps:**

1. Create a configuration file named `quesma.yaml` with the following content, make sure to replace placeholders (`#PLACE_YOUR*`) with actual values.
    ```yaml
    licenseKey: #PLACE_YOUR_LICENSE_KEY_HERE 
    # license key is required for backend connector of `hydrolix` type, 
    # please contact Quesma support (support@quesma.com) to obtain yours  
    frontendConnectors:
      - name: elastic-query
        type: elasticsearch-fe-query
        config:
          listenPort: 8080
      - name: elastic-ingest
        type: elasticsearch-fe-ingest
        config:
          listenPort: 8080
    backendConnectors:
      - name: elasticsearch-instance
        type: elasticsearch
        config:
          url: #PLACE_YOUR_ELASTICSEARCH_URL_HERE, for example: http://192.168.0.7:9200
          user: #PLACE_YOUR_ELASTICSEARCH_USERNAME_HERE
          password: #PLACE_YOUR_ELASTICSEARCH_PASSWORD_HERE
      - name: hydrolix-instance
        type: hydrolix
        config:
          url: #PLACE_YOUR_HYDROLIX_URL_HERE, for example: clickhouse://companyname.hydrolix.live:9440
          user: #PLACE_YOUR_HYDROLIX_USER_HERE
          password: #PLACE_YOUR_HYDROLIX_PASSWORD_HERE
          database: #PLACE_YOUR_HYDROLIX_DATABASE_NAME_HERE
    processors:
      - name: query-processor
        type: quesma-v1-processor-query
        config:
          indexes:  # the list below is just an example, 
            siem:   # make sure to replace them with your actual table or index names
              target:
                - hydrolix-instance
            logs:
              target:
                - hydrolix-instance
            index1:
              target:
                - elasticsearch-instance
            index2:
              target:
                - elasticsearch-instance
            index3:
              target:
                - elasticsearch-instance
            '*':       # DO NOT remove, always required
              target:
                - elasticsearch-instance
      - name: ingest-processor
        type: quesma-v1-processor-ingest
        config:
          indexes:    # the list below is just an example, 
            index1:   # make sure to replace them with your actual table or index names
              target:
                - elasticsearch-instance
            index2:
              target:
                - elasticsearch-instance
            index3:
              target:
                - elasticsearch-instance
            '*':       # DO NOT remove, always required
              target:
                - elasticsearch-instance
    pipelines:
      - name: elasticsearch-proxy-read
        frontendConnectors: [ elastic-query ]
        processors: [ query-processor ]
        backendConnectors: [ elasticsearch-instance, hydrolix-instance ]
      - name: elasticsearch-proxy-write
        frontendConnectors: [ elastic-ingest ]
        processors: [ ingest-processor ]
        backendConnectors: [ elasticsearch-instance, hydrolix-instance ] 
    ```
> Note: To learn more about configuration options, refer to [Configuration primer](/config-primer.md)
    
2. Run Quesma with the following command:
    ```bash
    docker run --name quesma -p 8080:8080 \
     -e QUESMA_CONFIG_FILE=/configuration/quesma.yaml \
    -v $(pwd)/quesma.yaml:/configuration/quesma.yaml quesma/quesma:latest 
    ```
   You now have a running Quesma instance running with Elasticsearch API endpoint on port 8080. You can also enable Quesma's admin panel at [localhost:9999](http://localhost:9999/) by adding `-p 9999:9999` to docker run command.
3. Reconfigure client endpoint:
   * For Kibana: update your [Kibana configuration](https://www.elastic.co/guide/en/kibana/current/settings.html), so that it points to Quesma Elasticsearch API endpoint mentioned above, instead of Elasticsearch original endpoint. In your Kibana configuration file, replace the `elasticsearch.hosts` value with Quesma's host and port, e.g.:
       ```yaml
      elasticsearch.hosts: ["http://quesma:8080"]
      ```
      or optionally using `ELASTICSEARCH_HOSTS` environment variable.

      If you use Elasticsearch/Kibana without authentication, please modify the `frontendConnectors` section in the following way:
      ```yaml
      frontendConnectors:
        - name: elastic-query
          type: elasticsearch-fe-query
          config:
            listenPort: 8080
            disableAuth: true
        - name: elastic-ingest
          type: elasticsearch-fe-ingest
          config:
            listenPort: 8080
            disableAuth: true
      ```
   * For OpenSearchDashboards: modify [`opensearch_dashboards.yml` file](https://opensearch.org/docs/latest/install-and-configure/configuring-dashboards/) and change `opensearch.hosts` property.

      If you use OpenSearch/OpenSearchDashboards without authentication, please modify the `frontendConnectors` section in the following way:
      ```yaml
      frontendConnectors:
        - name: elastic-query
          type: elasticsearch-fe-query
          config:
            listenPort: 8080
            disableAuth: true
        - name: elastic-ingest
          type: elasticsearch-fe-ingest
          config:
            listenPort: 8080
            disableAuth: true
      ```
4. Restart Kibana/OSD.
5. Add DataViews/Index Patterns:
   * For Kibana: in order to view your Hydrolix tables in Kibana, you need to create **Data Views** for tables (indexes) from the config. If you're unsure how to do it, follow the [Data Views creation guide](./adding-kibana-dataviews.md) for more information.
   * For OpenSearchDashboards: you may need to add **Index Patterns**. See [instructions](https://opensearch.org/docs/latest/dashboards/management/index-patterns/).
