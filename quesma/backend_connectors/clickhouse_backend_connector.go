// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"context"
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"

	quesma_api "github.com/QuesmaOrg/quesma/v2/core"
)

type ClickHouseBackendConnector struct {
	Endpoint   string
	connection *sql.DB
}

type ClickHouseRows struct {
	rows *sql.Rows
}
type ClickHouseRow struct {
	row *sql.Row
}

func (p *ClickHouseRow) Scan(dest ...interface{}) error {
	if p.row == nil {
		return sql.ErrNoRows
	}
	return p.row.Scan(dest...)
}

func (p *ClickHouseRows) Next() bool {
	return p.rows.Next()
}

func (p *ClickHouseRows) Scan(dest ...interface{}) error {
	return p.rows.Scan(dest...)
}

func (p *ClickHouseRows) Close() error {
	return p.rows.Close()
}

func (p *ClickHouseRows) Err() error {
	return p.rows.Err()
}

func (p *ClickHouseBackendConnector) GetId() quesma_api.BackendConnectorType {
	return quesma_api.ClickHouseSQLBackend
}

func (p *ClickHouseBackendConnector) Open() error {
	conn, err := initDBConnection()
	if err != nil {
		return err
	}
	p.connection = conn
	return nil
}

func (p *ClickHouseBackendConnector) Close() error {
	if p.connection == nil {
		return nil
	}
	return p.connection.Close()
}

func (p *ClickHouseBackendConnector) Ping() error {
	return p.connection.Ping()
}

func (p *ClickHouseBackendConnector) Query(ctx context.Context, query string, args ...interface{}) (quesma_api.Rows, error) {
	rows, err := p.connection.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &ClickHouseRows{rows: rows}, nil
}

func (p *ClickHouseBackendConnector) QueryRow(ctx context.Context, query string, args ...interface{}) quesma_api.Row {
	return p.connection.QueryRowContext(ctx, query, args...)
}

func (p *ClickHouseBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	if len(args) == 0 {
		_, err := p.connection.ExecContext(ctx, query)
		return err
	}
	_, err := p.connection.ExecContext(ctx, query, args...)
	return err
}

func (p *ClickHouseBackendConnector) Stats() quesma_api.DBStats {
	stats := p.connection.Stats()
	return quesma_api.DBStats{
		MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections:    stats.OpenConnections,
	}
}

// func initDBConnection(c *config.QuesmaConfiguration, tlsConfig *tls.Config) *sql.DB {
func initDBConnection() (*sql.DB, error) {
	options := clickhouse.Options{Addr: []string{"localhost:9000"}}
	info := struct {
		Name    string
		Version string
	}{
		Name:    "quesma",
		Version: "NEW ODD VERSION", //buildinfo.Version,
	}
	options.ClientInfo.Products = append(options.ClientInfo.Products, info)
	return clickhouse.OpenDB(&options), nil

}

func NewClickHouseBackendConnector(endpoint string) *ClickHouseBackendConnector {
	return &ClickHouseBackendConnector{
		Endpoint: endpoint,
	}
}

// NewClickHouseBackendConnectorWithConnection bridges the gap between the ClickHouseBackendConnector and the sql.DB
// so that it is can be used in pre-v2 code. Should be removed when moving forwards.
func NewClickHouseBackendConnectorWithConnection(endpoint string, conn *sql.DB) *ClickHouseBackendConnector {
	return &ClickHouseBackendConnector{
		Endpoint:   endpoint,
		connection: conn,
	}
}

func (p *ClickHouseBackendConnector) InstanceName() string {
	return "clickhouse" // TODO add name taken from config
}
