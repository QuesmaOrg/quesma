package testcases

import (
	"bytes"
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"
)

// InternalDockerHost (`host.testcontainers.internal`) doesn't work for Docker Desktop for Mac.
const InternalDockerHost = "host.docker.internal"
const configTemplatesDir = "configs"

type Containers struct {
	Elasticsearch *testcontainers.Container
	Quesma        *testcontainers.Container
	Kibana        *testcontainers.Container
	ClickHouse    *testcontainers.Container
}

func (c *Containers) Cleanup(ctx context.Context) {
	if c.Elasticsearch != nil {
		(*c.Elasticsearch).Terminate(ctx)
	}
	if c.Quesma != nil {
		(*c.Quesma).Terminate(ctx)
	}
	if c.Kibana != nil {
		(*c.Kibana).Terminate(ctx)
	}
	if c.ClickHouse != nil {
		(*c.ClickHouse).Terminate(ctx)
	}
}

func setupElasticsearch(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "docker.elastic.co/elasticsearch/elasticsearch:8.11.1",
		ExposedPorts: []string{"9200/tcp", "9300/tcp"},
		// Do i ned
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "true",
			"ELASTIC_USERNAME":       "elastic",
			"ELASTIC_PASSWORD":       "quesmaquesma",
			"ES_JAVA_OPTS":           "-Xms1024m -Xmx1024m",
		},
		HostAccessPorts: []int{9200, 9300},
		WaitingFor:      wait.ForListeningPort("9200/tcp"),
	}
	elasticsearch, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	// Set password to Kibana system user
	if retCode, _, err := elasticsearch.Exec(ctx, []string{"curl", "-H", "Content-type: application/json", "-k", "-u", "elastic:quesmaquesma", "http://localhost:9200/_security/user/kibana_system/_password", "-d", "{\"password\": \"kibanana\"}"}); retCode != 0 || err != nil {
		panic(fmt.Sprintf("Failed to set password for kibana_system: returned=[%d] err=[%v]", retCode, err))
	}

	return elasticsearch, nil
}

func setupQuesma(ctx context.Context, quesmaConfig string) (testcontainers.Container, error) {
	absPath, err := filepath.Abs(filepath.Join(".", configTemplatesDir, strings.TrimSuffix(quesmaConfig, ".template")))
	if err != nil {
		return nil, err
	}
	r, err := os.Open(absPath)
	if err != nil {
		return nil, err
	}
	quesmaReq := testcontainers.ContainerRequest{
		Image:        "quesma/quesma:latest",
		ExposedPorts: []string{"9999/tcp", "8080/tcp"},
		Env: map[string]string{
			"QUESMA_CONFIG_FILE": "/configuration/conf.yaml",
		},
		WaitingFor: wait.ForExposedPort().WithStartupTimeout(2 * time.Minute),
		Files: []testcontainers.ContainerFile{
			{
				Reader:            r,
				HostFilePath:      absPath,
				ContainerFilePath: "/configuration/conf.yaml",
				FileMode:          0o700,
			},
		},
	}

	quesma, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: quesmaReq,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	return quesma, nil
}

func setupKibana(ctx context.Context, quesmaContainer testcontainers.Container) (testcontainers.Container, error) {

	port, err := quesmaContainer.MappedPort(ctx, "8080/tcp")
	if err != nil {
		return nil, err
	}

	req := testcontainers.ContainerRequest{
		Image:        "docker.elastic.co/kibana/kibana:8.11.1",
		ExposedPorts: []string{"5601/tcp"},
		Env: map[string]string{
			"ELASTICSEARCH_HOSTS":                       fmt.Sprintf("[\"%s\"]", fmt.Sprintf("http://%s:%s", InternalDockerHost, port.Port())),
			"XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY": "QUESMAQUESMAQUESMAQUESMAQUESMAQUESMAQUESMAQUESMA",
			"ELASTICSEARCH_SSL_VERIFICATIONMODE":        "none",
			"ELASTICSEARCH_USERNAME":                    "kibana_system",
			"ELASTICSEARCH_PASSWORD":                    "kibanana",
			"XPACK_SECURITY_ENABLED":                    "true",
		},
		WaitingFor: wait.ForLog("http server running at").WithStartupTimeout(4 * time.Minute),
	}
	kibana, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	return kibana, nil
}

func setupClickHouse(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:24.5.3.5-alpine",
		ExposedPorts: []string{"8123/tcp", "9000/tcp"},
		WaitingFor:   wait.ForExposedPort().WithStartupTimeout(2 * time.Minute),
	}
	clickhouse, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	return clickhouse, nil
}

func RenderQuesmaConfig(configTemplate string, data map[string]string) error {
	absPath, err := filepath.Abs(filepath.Join(".", configTemplatesDir, configTemplate))
	content, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("error reading YAML file: %v", err)
	}
	tmpl, err := template.New("yamlTemplate").Parse(string(content))
	if err != nil {
		return fmt.Errorf("error creating template: %v", err)
	}
	var renderedContent bytes.Buffer
	err = tmpl.Execute(&renderedContent, data)
	if err != nil {
		return fmt.Errorf("error executing template: %v", err)
	}
	err = os.WriteFile(strings.TrimSuffix(absPath, ".template"), renderedContent.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("error writing rendered YAML file: %v", err)
	}
	return nil
}

func setupContainersForTransparentProxy(ctx context.Context, quesmaConfigTemplate string) (*Containers, error) {
	elasticsearch, err := setupElasticsearch(ctx)
	if err != nil {
		log.Fatalf("Failed to start Elasticsearch container: %s", err)
	}

	esPort, _ := elasticsearch.MappedPort(ctx, "9200/tcp")
	data := map[string]string{
		"elasticsearch_host": InternalDockerHost,
		"elasticsearch_port": esPort.Port(),
	}
	if err := RenderQuesmaConfig(quesmaConfigTemplate, data); err != nil {
		log.Fatalf("Failed to render Quesma config: %s", err)
	}

	quesma, err := setupQuesma(ctx, quesmaConfigTemplate)
	if err != nil {
		println(err)
	}

	kibana, err := setupKibana(ctx, quesma)
	if err != nil {
		log.Fatalf("Failed to start Kibana container: %s", err)
	}

	return &Containers{
		Elasticsearch: &elasticsearch,
		Quesma:        &quesma,
		Kibana:        &kibana,
		ClickHouse:    nil,
	}, nil
}

func setupAllContainersWithCh(ctx context.Context, quesmaConfigTemplate string) (*Containers, error) {
	elasticsearch, err := setupElasticsearch(ctx)
	if err != nil {
		log.Fatalf("Failed to start Elasticsearch container: %s", err)
	}
	esPort, _ := elasticsearch.MappedPort(ctx, "9200/tcp")

	clickhouse, err := setupClickHouse(ctx)
	if err != nil {
		log.Fatalf("Failed to start ClickHouse container: %s", err)
	}
	chPort, _ := clickhouse.MappedPort(ctx, "9000/tcp")

	data := map[string]string{
		"elasticsearch_host": InternalDockerHost,
		"elasticsearch_port": esPort.Port(),
		"clickhouse_host":    InternalDockerHost,
		"clickhouse_port":    chPort.Port(),
	}
	if err := RenderQuesmaConfig(quesmaConfigTemplate, data); err != nil {
		log.Fatalf("Failed to render Quesma config: %s", err)
	}

	quesma, err := setupQuesma(ctx, quesmaConfigTemplate)
	if err != nil {
		println(err)
	}

	kibana, err := setupKibana(ctx, quesma)
	if err != nil {
		log.Fatalf("Failed to start Kibana container: %s", err)
	}

	return &Containers{
		Elasticsearch: &elasticsearch,
		Quesma:        &quesma,
		Kibana:        &kibana,
		ClickHouse:    &clickhouse,
	}, nil
}
