// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"context"
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"

	quesma_api "quesma_v2/core"
)

type ClickHouseBackendConnector struct {
	Endpoint   string
	connection *sql.DB
}

type ClickHouseRows struct {
	rows *sql.Rows
}

func (p *ClickHouseRows) Next() bool {
	return p.rows.Next()
}

func (p *ClickHouseRows) Scan(dest ...interface{}) error {
	return p.rows.Scan(dest...)
}

func (p *ClickHouseRows) Close() {
	err := p.rows.Close()
	if err != nil {
		panic(err)
	}
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

func (p *ClickHouseBackendConnector) Query(ctx context.Context, query string, args ...interface{}) (quesma_api.Rows, error) {
	rows, err := p.connection.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &ClickHouseRows{rows: rows}, nil
}

func (p *ClickHouseBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	if len(args) == 0 {
		_, err := p.connection.ExecContext(ctx, query)
		return err
	}
	_, err := p.connection.ExecContext(ctx, query, args...)
	return err
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

func (p *ClickHouseBackendConnector) InstanceName() string {
	return "clickhouse" // TODO add name taken from config
}
