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
	"time"
)

type HydrolixBackendConnector struct {
	BasicSqlBackendConnector
	// TODO for now we still have reference for RelationalDbConfiguration for fallback
	cfg         *config.RelationalDbConfiguration
	IngestURL   string
	AccessToken string
	Headers     map[string]string
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
	return &HydrolixBackendConnector{
		cfg: configuration,
	}
}

func NewHydrolixBackendConnectorWithConnection(_ string, conn *sql.DB) *HydrolixBackendConnector {
	return &HydrolixBackendConnector{
		BasicSqlBackendConnector: BasicSqlBackendConnector{
			connection: conn,
		},
	}
}

func (p *HydrolixBackendConnector) InstanceName() string {
	return "hydrolix" // TODO add name taken from config
}

func isValidJSON(s string) bool {
	var js interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

func makeRequest(ctx context.Context, method string, url string, body []byte, token string, tableName string) (error, []byte) {
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
		return fmt.Errorf("ingest failed: %s — %s", resp.Status, string(respBody)), nil
	}
	return err, respBody
}

var tableId uuid.UUID
var transformCreated bool
var tableName string

func (p *HydrolixBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	token := "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIybDZyTk1YV2hYQTA5M2tkRHA5ZFctaEMzM2NkOEtWUFhJdURZLWlLeUFjIn0.eyJleHAiOjE3NTI2NTgxNDYsImlhdCI6MTc1MjU3MTc0NiwianRpIjoiODI2ZTViNjgtNWM4MS00NTUxLWI3N2EtOTZkNmVkNTM2ZTA1IiwiaXNzIjoiaHR0cHM6Ly9sb2NhbGhvc3Qva2V5Y2xvYWsvcmVhbG1zL2h5ZHJvbGl4LXVzZXJzIiwiYXVkIjpbImNvbmZpZy1hcGkiLCJhY2NvdW50Il0sInN1YiI6ImRiMWM1YTJiLTdhYjMtNGNmZi04NGU4LTQ3Yzc0YjRlZjAyMSIsInR5cCI6IkJlYXJlciIsImF6cCI6ImNvbmZpZy1hcGkiLCJzZXNzaW9uX3N0YXRlIjoiYWRlZTZjN2UtZmM4Yi00NzY4LTk5NTktY2FkY2Q3YWM5M2RjIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwOi8vbG9jYWxob3N0Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWh5ZHJvbGl4LXVzZXJzIiwib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIGNvbmZpZy1hcGktc2VydmljZSBlbWFpbCBwcm9maWxlIiwic2lkIjoiYWRlZTZjN2UtZmM4Yi00NzY4LTk5NTktY2FkY2Q3YWM5M2RjIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsInByZWZlcnJlZF91c2VybmFtZSI6Im1lQGh5ZHJvbGl4LmlvIiwiZW1haWwiOiJtZUBoeWRyb2xpeC5pbyJ9.F2NIUxD0lD-g5Q729jU5JN_ZfGQqvVZcHle3scMrG529czTfBZUoOBb90YFxPhHU7owzomTJ7v077UMTfgGuRUZNHy9CX2G33UB9Fy7RllgK-eW1MyFKxNdEDVkE-gESG7wtHqd-mxG_Yt9plXJs1wVHtrnYJ9GxKaWzpWaCMfFKq-rr6A9Ghuzr-FgWOvgTot9CExR8ThdOwVZREWXxhG0ki3bTnqQ1GRpwstORFPsPdJrtNaubZrGfqyjclMpLWRv4OVkDxQkAeW5ZcrjbtjdIed8Y1NIiWju74iOditHU4BiIfK82R8TlN112qMx8KjKq1gScpgjK8Jo6VKhrFA"
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
	var ingest map[string]interface{}

	if err := json.Unmarshal(root["create_table"], &createTable); err != nil {
		panic(err)
	}
	if err := json.Unmarshal(root["transform"], &transform); err != nil {
		panic(err)
	}
	if err := json.Unmarshal(root["ingest"], &ingest); err != nil {
		panic(err)
	}

	if len(createTable) > 0 && tableId == uuid.Nil {
		url := fmt.Sprintf("http://%s/config/v1/orgs/%s/projects/%s/tables/", hdxHost, orgID, projectID)
		tableName = createTable["name"].(string)
		if tableId == uuid.Nil {
			tableId = uuid.New()
		}
		createTable["uuid"] = tableId.String()
		createTableJson, err := json.Marshal(createTable)
		if err != nil {
			return fmt.Errorf("error marshalling create_table JSON: %v", err)
		}
		err, _ = makeRequest(ctx, "POST", url, createTableJson, token, tableName)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
			return err
		}
	}
	if len(transform) > 0 && !transformCreated {
		url := fmt.Sprintf("http://%s/config/v1/orgs/%s/projects/%s/tables/%s/transforms", hdxHost, orgID, projectID, tableId.String())
		transformJson, err := json.Marshal(transform)
		if err != nil {
			return fmt.Errorf("error marshalling transform JSON: %v", err)
		}

		err, _ = makeRequest(ctx, "POST", url, transformJson, token, tableName)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
			return err
		}
		time.Sleep(5 * time.Second) // Wait for the transform to be created
		transformCreated = true
	}
	if len(ingest) > 0 && transformCreated {
		ingestJson, err := json.Marshal(ingest)
		if err != nil {
			return fmt.Errorf("error marshalling ingest JSON: %v", err)
		}
		url := fmt.Sprintf("http://%s/ingest/event", hdxHost)
		err, _ = makeRequest(ctx, "POST", url, ingestJson, token, tableName)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
			return err
		}
	}

	return nil
}
