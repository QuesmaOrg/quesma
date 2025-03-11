// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"database/sql"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresBackendConnector struct {
	BasicSqlBackendConnector
	Endpoint string
}

func (p *PostgresBackendConnector) InstanceName() string {
	return "postgresql"
}

func (p *PostgresBackendConnector) GetId() quesma_api.BackendConnectorType {
	return quesma_api.PgSQLBackend
}

func (p *PostgresBackendConnector) Open() error {
	// Note: pgx library also has its own custom interface (pgx.Connect), which is not compatible
	// with the standard sql.DB interface, but has more features and is more efficient.
	conn, err := sql.Open("pgx", p.Endpoint)
	if err != nil {
		return err
	}
	err = conn.Ping()
	if err != nil {
		return err
	}
	p.connection = conn
	return nil
}

func NewPostgresBackendConnector(endpoint string) *PostgresBackendConnector {
	return &PostgresBackendConnector{
		Endpoint: endpoint,
	}
}
