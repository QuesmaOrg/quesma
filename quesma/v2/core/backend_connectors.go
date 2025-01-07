// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import "context"

type BackendConnectorType int

const (
	NoopBackend = iota
	MySQLBackend
	PgSQLBackend
	ClickHouseSQLBackend
	ElasticsearchBackend
)

func GetBackendConnectorNameFromType(connectorType BackendConnectorType) string {
	switch connectorType {
	case MySQLBackend:
		return "mysql"
	case PgSQLBackend:
		return "pgsql"
	case ClickHouseSQLBackend:
		return "clickhouse"
	case ElasticsearchBackend:
		return "elasticsearch"
	default:
		return "noop"
	}
}

type NoopBackendConnector struct {
}

func (p *NoopBackendConnector) InstanceName() string {
	return "noop"
}

func (p *NoopBackendConnector) GetId() BackendConnectorType {
	return NoopBackend
}

func (p *NoopBackendConnector) Open() error {
	return nil
}

func (p *NoopBackendConnector) Close() error {
	return nil
}

func (p *NoopBackendConnector) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return nil, nil
}

func (p *NoopBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	return nil
}

func (p *NoopBackendConnector) Ping() error {
	return nil
}
