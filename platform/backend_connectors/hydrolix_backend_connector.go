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
	cfg        *config.RelationalDbConfiguration
	client     *http.Client
	tableCache map[string]uuid.UUID
	tableMutex sync.Mutex
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

func checkHydrolixConfig(cfg *config.RelationalDbConfiguration) error {
	if cfg.Url == nil {
		return fmt.Errorf("hydrolix URL is not set")
	}
	if cfg.HydrolixToken == "" {
		return fmt.Errorf("hydrolix token is not set")
	}
	if cfg.HydrolixOrgId == "" {
		return fmt.Errorf("hydrolix organization ID is not set")
	}
	if cfg.HydrolixProjectId == "" {
		return fmt.Errorf("hydrolix project ID is not set")
	}
	return nil
}

func NewHydrolixBackendConnector(configuration *config.RelationalDbConfiguration) *HydrolixBackendConnector {
	if err := checkHydrolixConfig(configuration); err != nil {
		logger.Error().Msgf("Invalid Hydrolix configuration: %v", err)
		return nil
	}
	return &HydrolixBackendConnector{
		cfg: configuration,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
				DisableKeepAlives: true,
			},
		},
		tableCache: make(map[string]uuid.UUID),
	}
}

func NewHydrolixBackendConnectorWithConnection(configuration *config.RelationalDbConfiguration, conn *sql.DB) *HydrolixBackendConnector {
	if err := checkHydrolixConfig(configuration); err != nil {
		logger.Error().Msgf("Invalid Hydrolix configuration: %v", err)
		return nil
	}
	return &HydrolixBackendConnector{
		BasicSqlBackendConnector: BasicSqlBackendConnector{
			connection: conn,
		},
		cfg: configuration,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
				DisableKeepAlives: true,
			},
		},
		tableCache: make(map[string]uuid.UUID),
	}
}

func (p *HydrolixBackendConnector) InstanceName() string {
	return "hydrolix" // TODO add name taken from config
}

func isValidJSON(s string) bool {
	var js interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

func (p *HydrolixBackendConnector) makeRequest(ctx context.Context, method string, url string, body []byte, token string, tableName string) ([]byte, error) {
	// Build the request
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-hdx-table", "sample_project."+tableName)

	// Send the request
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ingest request failed: %s", err)
	}
	defer resp.Body.Close()

	// Read and print response
	respBody, err := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ingest failed: %s — %s", resp.Status, string(respBody))
	}
	return respBody, err
}

type HydrolixResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (p *HydrolixBackendConnector) ingestFun(ctx context.Context, ingestSlice []map[string]interface{}, tableName string, tableId string) error {
	logger.InfoWithCtx(ctx).Msgf("Ingests len: %s %d", tableName, len(ingestSlice))

	var data []json.RawMessage

	for _, row := range ingestSlice {
		if len(row) == 0 {
			continue
		}
		ingestJson, err := json.Marshal(row)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msg("Failed to marshal row")
			continue
		}
		data = append(data, ingestJson)
	}

	// Final payload: a JSON array of the rows
	finalJson, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal final JSON array: %w", err)
	}

	url := fmt.Sprintf("%s/ingest/event", p.cfg.Url.String())
	// Sleep duration is arbitrarily chosen.
	// It seems that the Hydrolix API needs some time to process the table creation before ingesting data.
	const sleepDuration = 5 * time.Second
	const maxRetries = 5
	for retries := 0; retries < maxRetries; retries++ {
		_, err := p.makeRequest(ctx, "POST", url, finalJson, p.cfg.HydrolixToken, tableName)
		if err != nil {
			logger.WarnWithCtx(ctx).Msgf("Error ingesting table %s: %v retrying...", tableName, err)
			time.Sleep(sleepDuration)
			continue
		}

		logger.InfoWithCtx(ctx).Msgf("Ingests successfull: %s %d", tableName, len(ingestSlice))
		return nil
	}
	return fmt.Errorf("failed to ingest after %d retries: %s", maxRetries, tableName)
}

func (p *HydrolixBackendConnector) getTableIdFromCache(tableName string) (uuid.UUID, bool) {
	p.tableMutex.Lock()
	defer p.tableMutex.Unlock()
	id, exists := p.tableCache[tableName]
	return id, exists
}

func (p *HydrolixBackendConnector) setTableIdInCache(tableName string, tableId uuid.UUID) {
	p.tableMutex.Lock()
	defer p.tableMutex.Unlock()
	p.tableCache[tableName] = tableId
}

func (p *HydrolixBackendConnector) createTableWithSchema(ctx context.Context,
	createTable map[string]interface{}, transform map[string]interface{},
	tableName string, tableId uuid.UUID) error {
	url := fmt.Sprintf("%s/config/v1/orgs/%s/projects/%s/tables/", p.cfg.Url.String(), p.cfg.HydrolixOrgId, p.cfg.HydrolixProjectId)
	createTableJson, err := json.Marshal(createTable)
	logger.Info().Msgf("createtable event: %s %s", tableName, string(createTableJson))

	if err != nil {
		return fmt.Errorf("error marshalling create_table JSON: %v", err)
	}
	_, err = p.makeRequest(ctx, "POST", url, createTableJson, p.cfg.HydrolixToken, tableName)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
		return err
	}

	url = fmt.Sprintf("%s/config/v1/orgs/%s/projects/%s/tables/%s/transforms", p.cfg.Url.String(), p.cfg.HydrolixOrgId, p.cfg.HydrolixProjectId, tableId.String())
	transformJson, err := json.Marshal(transform)
	if err != nil {
		return fmt.Errorf("error marshalling transform JSON: %v", err)
	}
	logger.Info().Msgf("transform event: %s %s", tableName, string(transformJson))

	_, err = p.makeRequest(ctx, "POST", url, transformJson, p.cfg.HydrolixToken, tableName)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("error making request: %v", err)
		return err
	}
	return nil
}

func (p *HydrolixBackendConnector) Exec(_ context.Context, query string, args ...interface{}) error {
	// TODO context might be cancelled too early
	ctx := context.Background()
	if !isValidJSON(query) {
		return fmt.Errorf("invalid JSON payload: %s", query)
	}

	// Top-level object
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(query), &root); err != nil {
		return err
	}

	// Extract each section into its own map (or struct, if needed)
	var createTable map[string]interface{}
	var transform map[string]interface{}
	var ingestSlice []map[string]interface{}

	if err := json.Unmarshal(root["create_table"], &createTable); err != nil {
		return err
	}
	if err := json.Unmarshal(root["transform"], &transform); err != nil {
		return err
	}
	if err := json.Unmarshal(root["ingest"], &ingestSlice); err != nil {
		return err
	}
	tableName := createTable["name"].(string)

	tableId, _ := p.getTableIdFromCache(tableName)
	if len(createTable) > 0 && tableId == uuid.Nil {
		tableId = uuid.New()
		createTable["uuid"] = tableId.String()
		err := p.createTableWithSchema(ctx, createTable, transform, tableName, tableId)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error creating table with schema: %v", err)
			return err
		}
		p.setTableIdInCache(tableName, tableId)
	}

	if len(ingestSlice) > 0 {
		logger.Info().Msgf("Received %d rows for table %s", len(ingestSlice), tableName)
		go p.ingestFun(ctx, ingestSlice, tableName, tableId.String())
	}

	return nil
}
