// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgproto3"
	"log"
	quesma_api "quesma_v2/core"
)

type PostgresToMySqlProcessor struct {
	BaseProcessor
}

func NewPostgresToMySqlProcessor() *PostgresToMySqlProcessor {
	return &PostgresToMySqlProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}

func (p *PostgresToMySqlProcessor) InstanceName() string {
	return "PostgresToMySqlProcessor"
}

func (p *PostgresToMySqlProcessor) GetId() string {
	return "postgrestomysql_processor"
}

func (p *PostgresToMySqlProcessor) respond() ([]byte, error) {
	result := []byte("QUESMA\n")
	backendConn := p.GetBackendConnector(quesma_api.MySQLBackend)
	if backendConn == nil {
		return result, nil
	}
	fmt.Println("Backend connector found")
	err := backendConn.Open()
	if err != nil {
		fmt.Printf("Error opening connection: %v", err)
	}
	// SQL query to select all users
	query := `SELECT id, username, email FROM users`

	// Execute the query
	rows, err := backendConn.Query(context.Background(), query)
	if err != nil {
		fmt.Printf("Failed to execute query: %v\n", err)
	}
	defer rows.Close()
	// Iterate over the rows

	for rows.Next() {
		var id int
		var username, email string
		err = rows.Scan(&id, &username, &email)
		if err != nil {
			log.Fatalf("Failed to scan row: %v\n", err)
		}

		res := fmt.Sprintf("User: ID=%d, Username=%s, Email=%s, CreatedAt=\n", id, username, email)
		fmt.Println(res)
		result = append(result, []byte(res)...)
	}

	// Check for any error that occurred during row iteration
	if err = rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v\n", err)
	}

	err = backendConn.Close()
	if err != nil {
		fmt.Printf("Error closing connection: %v", err)
	}

	return result, nil
}

func (p *PostgresToMySqlProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	fmt.Println("PostgresToMySql processor ")
	for _, m := range message {
		msg := m.(pgproto3.FrontendMessage)
		switch msg.(type) {
		case *pgproto3.Query:
			response, err := p.respond()
			if err != nil {
				return metadata, nil, fmt.Errorf("error generating query response: %w", err)
			}

			buf := mustEncode((&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("quesma"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          25,
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			}}).Encode(nil))
			buf = mustEncode((&pgproto3.DataRow{Values: [][]byte{response}}).Encode(buf))
			buf = mustEncode((&pgproto3.CommandComplete{CommandTag: []byte("")}).Encode(buf))
			buf = mustEncode((&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf))
			return metadata, buf, nil
		case *pgproto3.Terminate:
			return metadata, nil, nil

		default:
			fmt.Println("Received other than query")
			return metadata, nil, fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
	return metadata, nil, nil
}

func mustEncode(buf []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return buf
}

func (p *PostgresToMySqlProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.MySQLBackend}
}
