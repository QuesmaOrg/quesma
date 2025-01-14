// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"context"
	"database/sql"
	quesma_api "github.com/QuesmaOrg/quesma/v2/core"
	_ "github.com/go-sql-driver/mysql"
)

type MySqlRows struct {
	rows *sql.Rows
}

func (p *MySqlRows) Next() bool {
	return p.rows.Next()
}

func (p *MySqlRows) Scan(dest ...interface{}) error {
	return p.rows.Scan(dest...)
}

func (p *MySqlRows) Close() error {
	return p.rows.Close()
}

func (p *MySqlRows) Err() error {
	return p.rows.Err()
}

type MySqlBackendConnector struct {
	Endpoint   string
	connection *sql.DB
}

func (p *MySqlBackendConnector) InstanceName() string {
	return "mysql"
}

func (p *MySqlBackendConnector) GetId() quesma_api.BackendConnectorType {
	return quesma_api.MySQLBackend
}

func (p *MySqlBackendConnector) Open() error {
	conn, err := sql.Open("mysql", p.Endpoint)
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

func (p *MySqlBackendConnector) Close() error {
	if p.connection == nil {
		return nil
	}
	return p.connection.Close()
}

func (p *MySqlBackendConnector) Query(ctx context.Context, query string, args ...interface{}) (quesma_api.Rows, error) {
	rows, err := p.connection.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	return &MySqlRows{rows: rows}, nil
}

func (p *MySqlBackendConnector) QueryRow(ctx context.Context, query string, args ...interface{}) quesma_api.Row {
	return p.connection.QueryRowContext(ctx, query, args...)
}

func (p *MySqlBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	if len(args) == 0 {
		_, err := p.connection.ExecContext(context.Background(), query)
		return err
	}
	_, err := p.connection.ExecContext(context.Background(), query, args...)
	return err
}

func (p *MySqlBackendConnector) Stats() quesma_api.DBStats {
	stats := p.connection.Stats()
	return quesma_api.DBStats{
		MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections:    stats.OpenConnections,
	}
}

func (p *MySqlBackendConnector) Ping() error {
	return nil
}
