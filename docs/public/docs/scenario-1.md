# Scenario I: No ClickHouse installed

## Installation prerequisites

Make sure you have the following before proceeding:
1) Quesma Docker container image (later referred as `QUESMA_DOCKER_IMAGE`). To obtain one, email [support@quesma.com](mailto:support@quesma.com).
2) [Docker engine](https://docs.docker.com/engine/install/) installed (e.g. `docker ps` should pass).

If you are connecting Quesma to already installed and working backend databases, you also need credentials for them.

### Software and hardware requirements

We recommend setting up at least 2 CPU cores and 2GBs of RAM for the Quesma container. The requirements may vary depending on the workload and the number of concurrent connections.

Quesma has been tested with the following software versions:

| Software                         | Version         |
|----------------------------------|-----------------|
| Docker                           | `24.0.7`        | 
| Elasticsearch/Kibana             | `8.11.1`        |
| ClickHouse                       | `24.1`, `23.12` |
| OpenSearch/OpenSearch Dashboards | `2.12.0`        |
| Hydrolix                         | `v4.8.12`       |


## Installation steps

Quesma is delivered as a Docker container image. Follow these steps.
1) **Pull Quesma Docker image**
   ```shell
   export QUESMA_DOCKER_IMAGE=quesma/quesma:0.8.0-eap
   docker pull $QUESMA_DOCKER_IMAGE
   ```
   it should succeed.
2) **Run reference setup (recommended example Quesma configuration and docker compose files for Quesma, ClickHouse, and Kibana/Elastic or OpenSearch/OSDashboards), unless you have them already installed** \
   [Download the reference setup](https://eap.quesma.com/2024-05-10/reference.tgz) for Kibana or OpenSearch Dashboard:
   ```shell
   curl -O https://eap.quesma.com/2024-07-05/reference.tgz
   tar -xzvf reference.tgz
   cd reference/
   ```
   - Alternative 1: Kibana/Elasticsearch mode (runs Quesma, ClickHouse, Elasticsearch and Kibana)
      ```shell
      docker compose -f kibana.yml up -d
      ```
      You should see Kibana running on [`http://localhost:5601`](http://localhost:5601) and Quesma debugging interface [`http://localhost:9999`](http://localhost:9999/dashboard).
   - Alternative 2: OpenSearch Dashboard/OpenSearch mode (runs Quesma, ClickHouse, OpenSearch and OpenSearch Dashboards)
      ```shell
      docker compose -f osd.yml up -d
      ```
      You should see OpenSearch Dashboard running on [`http://localhost:5601`](http://localhost:5601) and Quesma debugging interface [`http://localhost:9999`](http://localhost:9999/dashboard).
  \
   
   In case you're using ClickHouse from the reference setup - it does not come with any data, but you can execute following commands to populate it with sample data:
   
   ```shell
   ## For Kibana 
   curl -w "HTTP %{http_code}" -k -o /dev/null --no-progress-meter -X POST "localhost:5601/api/sample_data/flights" \
    -H "kbn-xsrf: true" \
    -H 'Content-Type: application/json'

   ## For OpenSearch Dashboards
   curl -w "HTTP %{http_code}" -k -o /dev/null --no-progress-meter -X POST "localhost:5601/api/sample_data/flights" \
    -H "osd-xsrf: true" \
    -H 'Content-Type: application/json'
   ```
3) **Customize configuration file** \
   To connect Quesma to another database, you need to create new or modify existing the YAML configuration file.
   Edit the `quesma.yml` file and replace the Clickhouse url and credentials with the new target Clickhouse or Hydrolix url and credentials.
   Please refer to the Configuration section below.
   The file is referenced using the `QUESMA_CONFIG_FILE` environment variable. \
   You may apply the setup by one of the below commands:
    ```shell
    docker compose  -f kibana.yml restart
    docker compose  -f osd.yml restart
    ```
   Once you set up the `indexes` configuration section and have them pointing to your ClickHouse/Hydrolix tables, you can start querying your data!
   * **Kibana**
   Navigate to `Management` -> `Stack Management` -> `Data Views (in Kibana section)` and click `Create data view` to create your first data view with your tables. 
   * **OpenSearch Dashboards** \
   Navigate to `Dashboards Management` -> `Index Patterns` and click `Create Index Pattern` to create your first index pattern with your tables.
4) **Port the configuration to the production environment**
   Feel free to use the provided docker compose for your desired environment.
   You may want to use Terraform/OpenTofu, Pulumi, or other tools to automate the deployment.
   Quesma engineers are more than happy to help at [support@quesma.com](mailto:support@quesma.com).


## Configuration reference

Quesma can be configured using dedicated configuration files written in YAML format, which looks like this:
```yaml
port: 8080
logging:
  disableFileLogging: false
```
Additionally, you can use environment variables which **override** the configuration file settings. Please refer to the `Environment variables` section for more details.

### Configuration file

Quesma container has to have read access to the configuration file, which absolute path should be passed with `QUESMA_CONFIG_FILE` environment variable.
Configuration is being loaded at the start of the service and is **not** being reloaded during runtime. For any changes to take effect, Quesma restart is required.

We encourage you to take a look at **typical deployment scenarios** along with relevant example configurations 

For the full list of configuration options, please refer to the [Configuration primer](./config-primer.md).

### Environment variables

Environment variable names are case-sensitive and follow the pattern `QUESMA_<config-key>`, except here delimiter being `_` instead of `.`.

Examples:
* `QUESMA_logging_level=debug` overrides `logging.level` in the config file
* `QUESMA_licenseKey=xyz` overrides `licenseKey` in the config file
