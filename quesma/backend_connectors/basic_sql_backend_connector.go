// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"context"
	"database/sql"
	"github.com/QuesmaOrg/quesma/quesma/config"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

type SqlBackendConnector interface {
	GetDB() *sql.DB
}

type BasicSqlBackendConnector struct {
	connection *sql.DB
	cfg        *config.RelationalDbConfiguration
}

type SqlRows struct {
	rows *sql.Rows
}

func (p *SqlRows) Next() bool {
	return p.rows.Next()
}

func (p *SqlRows) Scan(dest ...interface{}) error {
	return p.rows.Scan(dest...)
}

func (p *SqlRows) Close() error {
	return p.rows.Close()
}

func (p *SqlRows) Err() error {
	return p.rows.Err()
}

func (p *BasicSqlBackendConnector) Open() error {
	conn, err := initDBConnection(p.cfg)
	if err != nil {
		return err
	}
	p.connection = conn
	return nil
}

func (p *BasicSqlBackendConnector) GetDB() *sql.DB {
	return p.connection
}

func (p *BasicSqlBackendConnector) Close() error {
	if p.connection == nil {
		return nil
	}
	return p.connection.Close()
}

func (p *BasicSqlBackendConnector) Ping() error {
	return p.connection.Ping()
}

func (p *BasicSqlBackendConnector) Query(ctx context.Context, query string, args ...interface{}) (quesma_api.Rows, error) {
	rows, err := p.connection.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SqlRows{rows: rows}, nil
}

func (p *BasicSqlBackendConnector) QueryRow(ctx context.Context, query string, args ...interface{}) quesma_api.Row {
	return p.connection.QueryRowContext(ctx, query, args...)
}

func (p *BasicSqlBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	if len(args) == 0 {
		_, err := p.connection.ExecContext(ctx, query)
		return err
	}
	_, err := p.connection.ExecContext(ctx, query, args...)
	return err
}

func (p *BasicSqlBackendConnector) Stats() quesma_api.DBStats {
	stats := p.connection.Stats()
	return quesma_api.DBStats{
		MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections:    stats.OpenConnections,
	}
}
