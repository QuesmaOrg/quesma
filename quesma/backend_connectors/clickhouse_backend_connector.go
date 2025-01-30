// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"

	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

type ClickHouseBackendConnector struct {
	BasicSqlBackendConnector
	Endpoint string
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
		BasicSqlBackendConnector: BasicSqlBackendConnector{
			connection: conn,
		},
		Endpoint: endpoint,
	}
}

func (p *ClickHouseBackendConnector) InstanceName() string {
	return "clickhouse" // TODO add name taken from config
}
