package backend_connectors

import (
	"context"
	"github.com/jackc/pgx/v4"
	quesma_api "quesma_v2/core"
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
	return p.connection.Query(context.Background(), query, args...)
}

func (p *PostgresBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	if len(args) == 0 {
		_, err := p.connection.Exec(context.Background(), query)
		return err
	}
	_, err := p.connection.Exec(context.Background(), query, args...)
	return err
}
