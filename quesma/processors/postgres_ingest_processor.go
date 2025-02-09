// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"context"
	"fmt"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/google/uuid"
	"log"
)

type PostgresIngestProcessor struct {
	BaseProcessor
}

func NewPostgresIngestProcessor() *PostgresIngestProcessor {
	return &PostgresIngestProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}

func (p *PostgresIngestProcessor) GetId() string {
	return "postgresingest"
}

func (p *PostgresIngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	for _, m := range message {
		mCasted, err := quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("PostgresQueryProcessor: invalid message type")
		}
		data = mCasted
		fmt.Println("PostgresIngest processor ")
		data = append(data, []byte("\nProcessed by PostgresIngest processor\n")...)
		data = append(data, []byte("\t|\n")...)
		backendConn := p.GetBackendConnector(quesma_api.PgSQLBackend)
		if backendConn == nil {
			fmt.Println("Backend connector not found")
			return metadata, data, nil
		}

		err = backendConn.Open()
		if err != nil {
			fmt.Printf("Error opening connection: %v", err)
			return nil, nil, err
		}
		createTableSQL := `
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    `

		err = backendConn.Exec(context.Background(), createTableSQL)
		if err != nil {
			log.Fatalf("Failed to create table: %v\n", err)
		}

		id := uuid.New()
		username := "user" + id.String()
		email := username + "@quesma.com"

		// Insert data into the users table
		insertSQL := `INSERT INTO users (username, email) VALUES ($1, $2)`

		err = backendConn.Exec(context.Background(), insertSQL, username, email)
		if err != nil {
			fmt.Printf("Error inserting data: %v", err)
			return nil, nil, err
		}
		data = append(data, []byte(fmt.Sprintf("\tUser: ID=%s, Username=%s, Email=%s, CreatedAt=\n", id, username, email))...)
		err = backendConn.Close()
		if err != nil {
			fmt.Printf("Error closing connection: %v", err)
			return nil, nil, err
		}
	}
	return metadata, data, nil
}

func (p *PostgresIngestProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.PgSQLBackend}
}
