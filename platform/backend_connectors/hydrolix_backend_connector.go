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
	}
	return respBody, err
}

var tableId uuid.UUID
var tableName string
var tableCache = make(map[string]uuid.UUID)
var tableMutex sync.Mutex

func (p *HydrolixBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	token := ""
	hdxHost := ""
	orgID := ""
	projectID := ""

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
		tableName = createTable["name"].(string)
		tableId = uuid.New()
		createTable["uuid"] = tableId.String()
		createTableJson, err := json.Marshal(createTable)
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

		_, err = makeRequest(ctx, "POST", url, transformJson, token, tableName)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
			return err
		}
		time.Sleep(5 * time.Second) // Wait for the transform to be created
	}
	if len(ingest) > 0 && tableId != uuid.Nil {
		ingestJson, err := json.Marshal(ingest)
		if err != nil {
			return fmt.Errorf("error marshalling ingest JSON: %v", err)
		}
		url := fmt.Sprintf("http://%s/ingest/event", hdxHost)
		_, err = makeRequest(ctx, "POST", url, ingestJson, token, tableName)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
			return err
		}
	}

	return nil
}
