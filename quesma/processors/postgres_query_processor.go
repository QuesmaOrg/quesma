// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"context"
	"fmt"
	quesma_api "github.com/QuesmaOrg/quesma/v2/core"
	"log"
)

type PostgresQueryProcessor struct {
	BaseProcessor
}

func NewPostgresQueryProcessor() *PostgresQueryProcessor {
	return &PostgresQueryProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}

func (p *PostgresQueryProcessor) GetId() string {
	return "postgresquery"
}

func (p *PostgresQueryProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	for _, m := range message {
		mCasted, err := quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("PostgresQueryProcessor: invalid message type")
		}
		data = mCasted
		fmt.Println("PostgresQuery processor ")
		data = append(data, []byte("Processed by Postgres Query processor\n")...)
		data = append(data, []byte("\t|\n")...)
		backendConn := p.GetBackendConnector(quesma_api.PgSQLBackend)
		if backendConn == nil {
			fmt.Println("Backend connector not found")
			return metadata, data, nil
		}
		fmt.Println("Backend connector found")
		err = backendConn.Open()
		if err != nil {
			fmt.Printf("Error opening connection: %v", err)
			return nil, nil, err
		}
		// SQL query to select all users
		query := `SELECT id, username, email FROM users`

		// Execute the query
		rows, err := backendConn.Query(context.Background(), query)
		if err != nil {
			fmt.Printf("Failed to execute query: %v\n", err)
			return nil, nil, err
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
			res := fmt.Sprintf("\tUser: ID=%d, Username=%s, Email=%s, CreatedAt=\n", id, username, email)
			fmt.Println(res)
			data = append(data, []byte(res)...)
		}

		// Check for any error that occurred during row iteration
		if err = rows.Err(); err != nil {
			log.Fatalf("Row iteration error: %v\n", err)
		}

		err = backendConn.Close()
		if err != nil {
			fmt.Printf("Error closing connection: %v", err)
			return nil, nil, err
		}
	}
	return metadata, data, nil
}

func (p *PostgresQueryProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.PgSQLBackend}
}
