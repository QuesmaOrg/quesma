// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/google/uuid"
	"io"
	"net/http"
	"sync"
	"time"
)

type HydrolixBackendConnector struct {
	BasicSqlBackendConnector
	// TODO for now we still have reference for RelationalDbConfiguration for fallback
	cfg             *config.RelationalDbConfiguration
	IngestURL       string
	AccessToken     string
	Headers         map[string]string
	createTableChan chan string
}

func (p *HydrolixBackendConnector) GetId() quesma_api.BackendConnectorType {
	return quesma_api.HydrolixSQLBackend
}

func (p *HydrolixBackendConnector) Open() error {
	conn, err := initDBConnection(p.cfg)
	if err != nil {
		return err
	}
	p.connection = conn
	return nil
}

func NewHydrolixBackendConnector(configuration *config.RelationalDbConfiguration) *HydrolixBackendConnector {
	createTableChan := make(chan string)
	go listenForCreateTable(createTableChan)
	return &HydrolixBackendConnector{
		cfg:             configuration,
		createTableChan: createTableChan,
	}
}

func NewHydrolixBackendConnectorWithConnection(_ string, conn *sql.DB) *HydrolixBackendConnector {
	createTableChan := make(chan string)
	go listenForCreateTable(createTableChan)
	return &HydrolixBackendConnector{
		BasicSqlBackendConnector: BasicSqlBackendConnector{
			connection: conn,
		},
		createTableChan: createTableChan,
	}
}

func (p *HydrolixBackendConnector) InstanceName() string {
	return "hydrolix" // TODO add name taken from config
}

func isValidJSON(s string) bool {
	var js interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

func makeRequest(ctx context.Context, method string, url string, body []byte, token string, tableName string) ([]byte, error) {
	// Build the request
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
	if err != nil {
		panic(err)
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-hdx-table", "sample_project."+tableName)

	// Allow self-signed certs (equivalent to curl -k)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	// Read and print response
	respBody, err := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ingest failed: %s — %s", resp.Status, string(respBody))
	} else {
		logger.InfoWithCtx(ctx).Msgf("Ingest successful: %s  %s — %s", tableName, resp.Status, string(respBody))
	}
	return respBody, err
}

var tableCache = make(map[string]uuid.UUID)
var tableMutex sync.Mutex

func listenForCreateTable(ch <-chan string) {
	for url := range ch {
		_ = url // TODO: handle the URL if needed
	}
}

func (p *HydrolixBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	token := "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIybDZyTk1YV2hYQTA5M2tkRHA5ZFctaEMzM2NkOEtWUFhJdURZLWlLeUFjIn0.eyJleHAiOjE3NTI4MzE4ODUsImlhdCI6MTc1Mjc0NTQ4NSwianRpIjoiMjc2NDNlMmYtYTEyMS00OTE4LWE3MjAtYzM3ZjZkNTA5NjQzIiwiaXNzIjoiaHR0cHM6Ly9sb2NhbGhvc3Qva2V5Y2xvYWsvcmVhbG1zL2h5ZHJvbGl4LXVzZXJzIiwiYXVkIjpbImNvbmZpZy1hcGkiLCJhY2NvdW50Il0sInN1YiI6ImRiMWM1YTJiLTdhYjMtNGNmZi04NGU4LTQ3Yzc0YjRlZjAyMSIsInR5cCI6IkJlYXJlciIsImF6cCI6ImNvbmZpZy1hcGkiLCJzZXNzaW9uX3N0YXRlIjoiMGEyMzNhZGItNjg4ZC00NDY3LWJkMmItMjQxOWRlZDk2MzNjIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwOi8vbG9jYWxob3N0Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWh5ZHJvbGl4LXVzZXJzIiwib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIGNvbmZpZy1hcGktc2VydmljZSBlbWFpbCBwcm9maWxlIiwic2lkIjoiMGEyMzNhZGItNjg4ZC00NDY3LWJkMmItMjQxOWRlZDk2MzNjIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsInByZWZlcnJlZF91c2VybmFtZSI6Im1lQGh5ZHJvbGl4LmlvIiwiZW1haWwiOiJtZUBoeWRyb2xpeC5pbyJ9.Zcer0PxmVB9cFe9_p4ubnBcIn4TjDBEXZoezjDoh9CduhyWVPoR3hRghO3JzkqpMavZxrijHlCsFbR31JKk3YZnuJ66Ve_7YwL8FJyzwVg17biW7ESAqRv0cYpmoUIh5AsQT8sagLhdrvX4wmndJvsrGJiGsYn6-YFj-R4Q7qyK4HAGk2IfyRlTeqWSN6FC1y_jgr4IqXB5gU6Y5pnTs782yx-0qd8rMGb6a3h4OFeSz2qS-y0zcDRV8pxyE27RRiN1-cQIL90QtMUMrAcy_qp-YnY15kr_xGbjMpbvDvL-R6xaxHH1DBIfs-QAdgj0IgMe-EBO9-w3h7Vpxmyda1w"
	hdxHost := "3.20.203.177:8888"
	orgID := "d9ce0431-f26f-44e3-b0ef-abc1653d04eb"
	projectID := "27506b30-0c78-41fa-a059-048d687f1164"

	if !isValidJSON(query) {
		return fmt.Errorf("invalid JSON payload: %s", query)
	}

	// Top-level object
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(query), &root); err != nil {
		panic(err)
	}

	// Extract each section into its own map (or struct, if needed)
	var createTable map[string]interface{}
	var transform map[string]interface{}
	var ingest []map[string]interface{}

	if err := json.Unmarshal(root["create_table"], &createTable); err != nil {
		panic(err)
	}
	if err := json.Unmarshal(root["transform"], &transform); err != nil {
		panic(err)
	}
	if err := json.Unmarshal(root["ingest"], &ingest); err != nil {
		panic(err)
	}
	var tableId uuid.UUID
	// Check if tableId is already cached
	tableMutex.Lock()
	if id, exists := tableCache[createTable["name"].(string)]; exists {
		tableId = id
	} else {
		tableId = uuid.Nil
	}
	tableMutex.Unlock()

	if len(createTable) > 0 && tableId == uuid.Nil {
		url := fmt.Sprintf("http://%s/config/v1/orgs/%s/projects/%s/tables/", hdxHost, orgID, projectID)
		tableName := createTable["name"].(string)
		tableId = uuid.New()
		createTable["uuid"] = tableId.String()
		createTableJson, err := json.Marshal(createTable)
		logger.Info().Msgf("createtable event: %s %s", createTable["name"].(string), string(createTableJson))

		if err != nil {
			return fmt.Errorf("error marshalling create_table JSON: %v", err)
		}
		_, err = makeRequest(ctx, "POST", url, createTableJson, token, tableName)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
			return err
		}

		url = fmt.Sprintf("http://%s/config/v1/orgs/%s/projects/%s/tables/%s/transforms", hdxHost, orgID, projectID, tableId.String())
		transformJson, err := json.Marshal(transform)
		if err != nil {
			return fmt.Errorf("error marshalling transform JSON: %v", err)
		}
		logger.Info().Msgf("transform event: %s %s", createTable["name"].(string), string(transformJson))

		_, err = makeRequest(ctx, "POST", url, transformJson, token, tableName)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
			return err
		}
		tableMutex.Lock()
		tableCache[createTable["name"].(string)] = tableId
		tableMutex.Unlock()
		time.Sleep(5 * time.Second) // Wait for the transform to be created
	}

	if len(ingest) > 0 {
		logger.Info().Msgf("ingests len: %s %d", createTable["name"].(string), len(ingest))
		for _, row := range ingest {
			if len(row) == 0 {
				continue
			}
			ingestJson, err := json.Marshal(row)
			if err != nil {
				return fmt.Errorf("error marshalling ingest JSON: %v", err)
			}
			url := fmt.Sprintf("http://%s/ingest/event", hdxHost)
			logger.Info().Msgf("ingest event: %s %s", createTable["name"].(string), string(ingestJson))
			_, err = makeRequest(ctx, "POST", url, ingestJson, token, createTable["name"].(string))
			if err != nil {
				logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
				return err
			}
		}
	}

	return nil
}
