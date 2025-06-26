// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"bytes"
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
	"time"
)

const (
	// debugQuesmaDuringTestRun should be set to `true` if you would like to run Quesma in IDE with debugger on
	// during the integration test run.
	debugQuesmaDuringTestRun = false
)

const configTemplatesDir = "configs"

func GetInternalDockerHost() string {
	return "localhost"
	//if check := os.Getenv("EXECUTING_ON_GITHUB_CI"); check != "" {
	//	return "localhost-for-github-ci"
	//}
	//return "host.docker.internal" // `host.testcontainers.internal` doesn't work for Docker Desktop for Mac.
}

type Containers struct {
	Elasticsearch *testcontainers.Container
	Quesma        *testcontainers.Container
	Kibana        *testcontainers.Container
	ClickHouse    *testcontainers.Container
}

// Read the last X bytes from the reader and return the last N lines from that
// Requires two readers because io.Reader doesn't support seeking
func tail(reader io.Reader, readerCopy io.Reader) ([]string, error) {
	// Size of chunk to read from the end (1MB)
	const chunkSize = 1024 * 1024
	// Maximum number of lines to return
	const maxLines = 1000

	totalSize, err := io.Copy(io.Discard, reader)
	if err != nil {
		return nil, err
	}

	skip := totalSize - chunkSize
	if skip > 0 {
		_, err = io.CopyN(io.Discard, readerCopy, skip)
		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	output, err := io.ReadAll(readerCopy)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(output), "\n")

	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}

	// The chunk could have started in the middle of a line, so we skip the first line
	if len(lines) > 0 {
		lines = lines[1:]
	}

	return lines, nil
}

func printContainerLogs(ctx context.Context, container *testcontainers.Container, name string) {
	if container == nil {
		return
	}

	reader, err := (*container).Logs(ctx)
	if err != nil {
		log.Printf("Failed to get logs for container '%s': %v", name, err)
		return
	}
	defer reader.Close()

	readerCopy, err := (*container).Logs(ctx)
	if err != nil {
		log.Printf("Failed to get logs for container '%s': %v", name, err)
		return
	}
	defer readerCopy.Close()

	lines, err := tail(reader, readerCopy)
	if err != nil {
		log.Printf("Failed to read logs for container '%s': %v", name, err)
		return
	}

	log.Printf("Logs for container '%s':", name)
	for _, line := range lines {
		log.Printf("[%s]: %s", name, line)
	}
}

func terminateContainer(ctx context.Context, container *testcontainers.Container, name string) {
	if container == nil {
		log.Printf("Container '%s' is nil", name)
		return
	}

	err := (*container).Terminate(ctx)
	if err != nil {
		log.Printf("Failed to terminate container '%s': %v", name, err)
	}
}

func (c *Containers) Cleanup(ctx context.Context, t *testing.T) {
	if t.Failed() {
		printContainerLogs(ctx, c.Elasticsearch, "Elasticsearch")
		printContainerLogs(ctx, c.Quesma, "Quesma")
		printContainerLogs(ctx, c.Kibana, "Kibana")
		printContainerLogs(ctx, c.ClickHouse, "ClickHouse")
	}

	terminateContainer(ctx, c.Elasticsearch, "Elasticsearch")
	terminateContainer(ctx, c.Quesma, "Quesma")
	terminateContainer(ctx, c.Kibana, "Kibana")
	terminateContainer(ctx, c.ClickHouse, "ClickHouse")
}

func setupElasticsearch(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "docker.elastic.co/elasticsearch/elasticsearch:8.11.1",
		ExposedPorts: []string{"0.0.0.0::9200/tcp", "0.0.0.0::9300/tcp"},
		// Do i ned
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "true",
			"ELASTIC_USERNAME":       "elastic",
			"ELASTIC_PASSWORD":       "quesmaquesma",
			"ES_JAVA_OPTS":           "-Xms1024m -Xmx1024m",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.ExtraHosts = []string{"localhost-for-github-ci:host-gateway"}
		},
		HostAccessPorts: []int{9200, 9300},
		WaitingFor: wait.ForHTTP("/").WithPort("9200").
			WithBasicAuth("elastic", "quesmaquesma").
			WithStartupTimeout(2 * time.Minute),
	}
	elasticsearch, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return elasticsearch, err
	}

	// Set password to Kibana system user
	if retCode, reader, errCmd := elasticsearch.Exec(ctx, []string{"curl", "-H", "Content-type: application/json", "-k", "-u", "elastic:quesmaquesma", "http://0.0.0.0:9200/_security/user/kibana_system/_password", "-d", "{\"password\": \"kibanana\"}"}); retCode != 0 || errCmd != nil {
		output := new(bytes.Buffer)
		output.ReadFrom(reader)
		log.Printf("Command output: %s", output.String())
		return elasticsearch, fmt.Errorf("Failed to set password for kibana_system: returned=[%d] err=[%v]", retCode, errCmd)
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

	quesmaVersion := os.Getenv("QUESMA_IT_VERSION")
	if quesmaVersion == "" {
		log.Println("No QUESMA_IT_VERSION environment variable set, watch out for stale images!")
		quesmaVersion = "nightly"
	}

	quesmaReq := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("quesma/quesma:%s", quesmaVersion),
		ExposedPorts: []string{"0.0.0.0::9999/tcp", "0.0.0.0::8080/tcp"},
		Env: map[string]string{
			"QUESMA_CONFIG_FILE": "/configuration/conf.yaml",
		},
		WaitingFor: wait.ForHTTP("/").WithPort("8080").
			WithBasicAuth("elastic", "quesmaquesma").
			WithStartupTimeout(2 * time.Minute),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.ExtraHosts = []string{"localhost-for-github-ci:host-gateway"}
		},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            r,
				HostFilePath:      absPath,
				ContainerFilePath: "/configuration/conf.yaml",
				FileMode:          0o700,
			},
		},
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: quesmaReq,
		Started:          true,
	})
}

func setupKibana(ctx context.Context, quesmaContainer testcontainers.Container) (testcontainers.Container, error) {

	port, err := quesmaContainer.MappedPort(ctx, "8080/tcp")
	if err != nil {
		return nil, err
	}

	req := testcontainers.ContainerRequest{
		Image:        "docker.elastic.co/kibana/kibana:8.11.1",
		ExposedPorts: []string{"0.0.0.0::5601/tcp"},
		Env: map[string]string{
			"ELASTICSEARCH_HOSTS":                       fmt.Sprintf("[\"%s\"]", fmt.Sprintf("http://%s:%s", GetInternalDockerHost(), port.Port())),
			"XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY": "QUESMAQUESMAQUESMAQUESMAQUESMAQUESMAQUESMAQUESMA",
			"ELASTICSEARCH_SSL_VERIFICATIONMODE":        "none",
			"ELASTICSEARCH_USERNAME":                    "kibana_system",
			"ELASTICSEARCH_PASSWORD":                    "kibanana",
			"XPACK_SECURITY_ENABLED":                    "true",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.ExtraHosts = []string{"localhost-for-github-ci:host-gateway"}
		},
		WaitingFor: wait.ForLog("http server running at").WithStartupTimeout(4 * time.Minute),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func setupClickHouse(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:24.5.3.5-alpine",
		ExposedPorts: []string{"0.0.0.0::8123/tcp", "0.0.0.0::9000/tcp"},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.ExtraHosts = []string{"localhost-for-github-ci:host-gateway"}
		},
		WaitingFor: wait.ForSQL("9000", "clickhouse",
			func(host string, port nat.Port) string {
				return fmt.Sprintf("clickhouse://%s:%d", host, port.Int())
			}).WithStartupTimeout(2 * time.Minute),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func RenderQuesmaConfig(configTemplate string, data map[string]string) (string, error) {
	absPath, err := filepath.Abs(filepath.Join(".", configTemplatesDir, configTemplate))
	content, err := os.ReadFile(absPath)
	if err != nil {
		return "", fmt.Errorf("error reading YAML file: %v", err)
	}
	tmpl, err := template.New("yamlTemplate").Parse(string(content))
	if err != nil {
		return "", fmt.Errorf("error creating template: %v", err)
	}
	var renderedContent bytes.Buffer
	err = tmpl.Execute(&renderedContent, data)
	if err != nil {
		return "", fmt.Errorf("error executing template: %v", err)
	}
	configPath := strings.TrimSuffix(absPath, ".template")
	err = os.WriteFile(configPath, renderedContent.Bytes(), 0644)
	if err != nil {
		return "", fmt.Errorf("error writing rendered YAML file: %v", err)
	}
	return configPath, nil
}

func setupContainersForTransparentProxy(ctx context.Context, quesmaConfigTemplate string) (*Containers, error) {
	containers := Containers{}

	elasticsearch, err := setupElasticsearch(ctx)
	containers.Elasticsearch = &elasticsearch
	if err != nil {
		return &containers, fmt.Errorf("failed to start Elasticsearch container: %s", err)
	}
	esPort, _ := elasticsearch.MappedPort(ctx, "9200/tcp")

	data := map[string]string{
		"elasticsearch_host": GetInternalDockerHost(),
		"elasticsearch_port": esPort.Port(),
	}
	if _, err := RenderQuesmaConfig(quesmaConfigTemplate, data); err != nil {
		return &containers, fmt.Errorf("failed to render Quesma config: %v", err)
	}

	quesma, err := setupQuesma(ctx, quesmaConfigTemplate)
	containers.Quesma = &quesma
	if err != nil {
		return &containers, fmt.Errorf("failed to start Quesma, %v", err)
	}

	kibana, err := setupKibana(ctx, quesma)
	containers.Kibana = &kibana
	if err != nil {
		return &containers, fmt.Errorf("failed to start Kibana container: %v", err)
	}

	return &containers, nil
}

func setupAllContainersWithCh(ctx context.Context, quesmaConfigTemplate string) (*Containers, error) {
	containers := Containers{}

	elasticsearch, err := setupElasticsearch(ctx)
	containers.Elasticsearch = &elasticsearch
	if err != nil {
		return &containers, fmt.Errorf("failed to start Elasticsearch container: %s", err)
	}

	esPort, _ := elasticsearch.MappedPort(ctx, "9200/tcp")

	clickhouse, err := setupClickHouse(ctx)
	containers.ClickHouse = &clickhouse
	if err != nil {
		return &containers, fmt.Errorf("failed to start ClickHouse container: %s", err)
	}

	chPort, _ := clickhouse.MappedPort(ctx, "9000/tcp")

	data := map[string]string{
		"elasticsearch_host": GetInternalDockerHost(),
		"elasticsearch_port": esPort.Port(),
		"clickhouse_host":    GetInternalDockerHost(),
		"clickhouse_port":    chPort.Port(),
	}
	configPath, err := RenderQuesmaConfig(quesmaConfigTemplate, data)
	if err != nil {
		return &containers, fmt.Errorf("failed to render Quesma config: %v", err)
	}

	debuggerQuesmaConfig := filepath.Join(filepath.Dir(configPath), "quesma-with-debugger.yml")
	content, err := os.ReadFile(configPath)
	if err != nil {
		return &containers, fmt.Errorf("failed to read rendered Quesma config: %v", err)
	}
	if err := os.WriteFile(debuggerQuesmaConfig, content, 0644); err != nil {
		return &containers, fmt.Errorf("failed to write dupa.yml: %v", err)
	}
	log.Printf("Quesma config rendered to: %s", debuggerQuesmaConfig)

	var quesma testcontainers.Container
	if debugQuesmaDuringTestRun {
		log.Printf("Waiting for you to start Quesma form your IDE using `Debug Quesma IT` configuration")
		for {
			if resp, err := http.Get("http://localhost:8080"); err == nil {
				resp.Body.Close()
				break
			}
			log.Printf("Waiting for Quesma HTTP server at port 8080...")
			time.Sleep(1 * time.Second)
			quesma = NewManuallyCreatedContainer()
		}
	} else {
		quesma, err = setupQuesma(ctx, debuggerQuesmaConfig)
		if err != nil {
			return &containers, fmt.Errorf("failed to start Quesma: %v", err)
		}
		containers.Quesma = &quesma
	}

	kibana, err := setupKibana(ctx, quesma)
	containers.Kibana = &kibana
	if err != nil {
		return &containers, fmt.Errorf("failed to start Kibana container: %v", err)
	}

	return &containers, nil
}
