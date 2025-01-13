// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"context"
	quesma_api "github.com/QuesmaOrg/quesma/v2/core"
	"github.com/jackc/pgx/v4"
)

type PostgresBackendConnector struct {
	Endpoint   string
	connection *pgx.Conn
}

func (p *PostgresBackendConnector) GetId() quesma_api.BackendConnectorType {
	return quesma_api.PgSQLBackend
}

func (p *PostgresBackendConnector) Open() error {
	conn, err := pgx.Connect(context.Background(), p.Endpoint)
	if err != nil {
		return err
	}
	p.connection = conn
	return nil
}

func (p *PostgresBackendConnector) Close() error {
	if p.connection == nil {
		return nil
	}
	return p.connection.Close(context.Background())
}

func (p *PostgresBackendConnector) Query(ctx context.Context, query string, args ...interface{}) (quesma_api.Rows, error) {
	pgRows, err := p.connection.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &PgRows{rows: pgRows}, nil
}

func (p *PostgresBackendConnector) QueryRow(ctx context.Context, query string, args ...interface{}) quesma_api.Row {
	return p.connection.QueryRow(ctx, query, args...)
}

func (p *PostgresBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	if len(args) == 0 {
		_, err := p.connection.Exec(ctx, query)
		return err
	}
	_, err := p.connection.Exec(ctx, query, args...)
	return err
}

func (p *PostgresBackendConnector) Stats() quesma_api.DBStats {
	return quesma_api.DBStats{}
}

type PgRows struct {
	rows pgx.Rows
}

func (p *PgRows) Next() bool {
	return p.rows.Next()
}

func (p *PgRows) Scan(dest ...interface{}) error {
	return p.rows.Scan(dest...)
}

func (p *PgRows) Close() error {
	p.rows.Close()
	return nil
}

func (p *PgRows) Err() error {
	return p.rows.Err()
}
