// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"database/sql"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"

	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
)

type DorisBackendConnector struct {
	BasicSqlBackendConnector
	cfg *config.RelationalDbConfiguration
}

func (p *DorisBackendConnector) GetId() quesma_api.BackendConnectorType {
	return quesma_api.DorisSQLBackend
}

func (p *DorisBackendConnector) Open() error {
	conn, err := initDorisDBConnection(p.cfg)
	if err != nil {
		return err
	}
	p.connection = conn
	return nil
}

func initDorisDBConnection(c *config.RelationalDbConfiguration) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true", c.User, c.Password, c.Url.Host, c.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize Doris connection pool")
	}

	if err := db.Ping(); err != nil {
		logger.Error().Err(err).Msg("failed to ping Doris server")
	}

	logger.Info().Msg("Doris connection pool initialized successfully")
	return db, nil
}

func NewDorisBackendConnector(configuration *config.RelationalDbConfiguration) *DorisBackendConnector {
	return &DorisBackendConnector{
		cfg: configuration,
	}
}

// NewClickHouseBackendConnectorWithConnection bridges the gap between the ClickHouseBackendConnector and the sql.DB
// so that it is can be used in pre-v2 code. Should be removed when moving forwards.
func NewDorisConnectorWithConnection(_ string, conn *sql.DB) *DorisBackendConnector {
	return &DorisBackendConnector{
		BasicSqlBackendConnector: BasicSqlBackendConnector{
			connection: conn,
		},
	}
}

func (p *DorisBackendConnector) InstanceName() string {
	return "doris" // TODO add name taken from config
}
