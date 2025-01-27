// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"crypto/tls"
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/QuesmaOrg/quesma/quesma/buildinfo"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"

	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

type ClickHouseBackendConnector struct {
	BasicSqlBackendConnector
	//Endpoint string
	cfg *config.RelationalDbConfiguration
}

func (p *ClickHouseBackendConnector) GetId() quesma_api.BackendConnectorType {
	return quesma_api.ClickHouseSQLBackend
}

func (p *ClickHouseBackendConnector) Open() error {
	conn, err := initDBConnection(p.cfg)
	if err != nil {
		return err
	}
	p.connection = conn
	return nil
}

// func initDBConnection(c *config.QuesmaConfiguration, tlsConfig *tls.Config) *sql.DB {
func initDBConnection(c *config.RelationalDbConfiguration) (*sql.DB, error) {
	options := clickhouse.Options{Addr: []string{c.Url.Host}}
	if c.User != "" || c.Password != "" || c.Database != "" {

		options.Auth = clickhouse.Auth{
			Username: c.User,
			Password: c.Password,
			Database: c.Database,
		}
	}
	if !c.DisableTLS {
		options.TLS = &tls.Config{InsecureSkipVerify: true} // TODO this should be changed according to `connection.go` (more or less)
	}

	info := struct {
		Name    string
		Version string
	}{
		Name:    "quesma",
		Version: buildinfo.Version,
	}

	options.ClientInfo.Products = append(options.ClientInfo.Products, info)
	return clickhouse.OpenDB(&options), nil

}

func NewClickHouseBackendConnector(configuration *config.RelationalDbConfiguration) *ClickHouseBackendConnector {
	return &ClickHouseBackendConnector{
		cfg: configuration,
	}
}

// NewClickHouseBackendConnectorWithConnection bridges the gap between the ClickHouseBackendConnector and the sql.DB
// so that it is can be used in pre-v2 code. Should be removed when moving forwards.
func NewClickHouseBackendConnectorWithConnection(_ string, conn *sql.DB) *ClickHouseBackendConnector {
	return &ClickHouseBackendConnector{
		BasicSqlBackendConnector: BasicSqlBackendConnector{
			connection: conn,
		},
		//Endpoint: endpoint,
	}
}

func (p *ClickHouseBackendConnector) InstanceName() string {
	return "clickhouse" // TODO add name taken from config
}
