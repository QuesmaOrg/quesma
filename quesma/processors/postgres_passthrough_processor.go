// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"context"
	"fmt"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/jackc/pgx/v5/pgproto3"
)

type PostgresPassthroughProcessor struct {
	BaseProcessor
}

func NewPostgresPassthroughProcessor() *PostgresPassthroughProcessor {
	return &PostgresPassthroughProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}

func (p *PostgresPassthroughProcessor) InstanceName() string {
	return "PostgresPassthroughProcessor"
}

func (p *PostgresPassthroughProcessor) GetId() string {
	return "postgrespassthrough_processor"
}

func (p *PostgresPassthroughProcessor) respond(query string) ([]byte, error) {
	backendConn := p.GetBackendConnector(quesma_api.PgSQLBackend)
	if backendConn == nil {
		return nil, fmt.Errorf("no backend connector found")
	}
	fmt.Println("Backend connector found")
	err := backendConn.Open()
	if err != nil {
		return nil, fmt.Errorf("error opening connection: %v", err)
	}
	defer backendConn.Close()

	// Execute the query
	rows, err := backendConn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// backendConn interface doesn't have a method to get column names, so we create dummy column names
	columnNames := []string{"col_1"}

	// Create field descriptions for each column
	fields := make([]pgproto3.FieldDescription, len(columnNames))
	for i, name := range columnNames {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(name),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          25, // Default to text type
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		}
	}

	// Create row description
	rowDesc := &pgproto3.RowDescription{Fields: fields}
	buf := mustEncode(rowDesc.Encode(nil))

	// Prepare scannable destination slice
	values := make([]interface{}, len(columnNames))
	valuePtrs := make([]interface{}, len(columnNames))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Iterate through rows
	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// Convert values to strings
		rowValues := make([][]byte, len(columnNames))
		for i, val := range values {
			if val == nil {
				rowValues[i] = nil
			} else {
				rowValues[i] = []byte(fmt.Sprintf("%v", val))
			}
		}

		// Encode each row
		dataRow := &pgproto3.DataRow{Values: rowValues}
		buf = mustEncode(dataRow.Encode(buf))
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %v", err)
	}

	// Add command complete and ready for query messages
	buf = mustEncode((&pgproto3.CommandComplete{CommandTag: []byte("SELECT")}).Encode(buf))
	buf = mustEncode((&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf))

	return buf, nil
}

func (p *PostgresPassthroughProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	fmt.Println("PostgresPassthrough processor ")
	for _, m := range message {
		msg := m.(pgproto3.FrontendMessage)
		switch msg := msg.(type) {
		case *pgproto3.Query:
			response, err := p.respond(msg.String)
			if err != nil {
				return metadata, nil, fmt.Errorf("error generating query response: %w", err)
			}
			return metadata, response, nil
		case *pgproto3.Terminate:
			return metadata, nil, nil

		default:
			fmt.Println("Received other than query")
			return metadata, nil, fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
	return metadata, nil, nil
}

func (p *PostgresPassthroughProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.PgSQLBackend}
}
