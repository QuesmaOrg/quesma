// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"database/sql"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	_ "github.com/go-sql-driver/mysql"
)

type MySqlBackendConnector struct {
	BasicSqlBackendConnector
	Endpoint string
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

func NewMySqlBackendConnector(endpoint string) *MySqlBackendConnector {
	return &MySqlBackendConnector{
		Endpoint: endpoint,
	}
}
