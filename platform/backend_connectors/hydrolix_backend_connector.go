// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"io"
	"net/http"

	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
)

type HydrolixBackendConnector struct {
	BasicSqlBackendConnector
	cfg         *config.RelationalDbConfiguration
	IngestURL   string
	AccessToken string
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

func (p *HydrolixBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	if p.IngestURL == "" || p.AccessToken == "" {
		logger.Info().Msg("missing ingest URL or access token")
		// TODO for fallback, execute the query directly on the database connection
		_, err := p.connection.ExecContext(ctx, query)
		return err
	}

	// Create HTTP request using the JSON payload from query
	req, err := http.NewRequestWithContext(ctx, "POST", p.IngestURL, bytes.NewBufferString(query))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.AccessToken)
	req.Header.Set("Content-Type", "application/json")

	// Execute HTTP request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Handle error response
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ingest failed: %s — %s", resp.Status, string(body))
	}

	return nil
}
