// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"context"
	"fmt"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/google/uuid"
)

type MySqlIngestProcessor struct {
	BaseProcessor
}

func NewMySqlIngestProcessor() *MySqlIngestProcessor {
	return &MySqlIngestProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}

func (p *MySqlIngestProcessor) GetId() string {
	return "postgresingest"
}

func (p *MySqlIngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	for _, m := range message {
		mCasted, err := quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("MySqlIngestProcessor: invalid message type")
		}
		data = mCasted
		fmt.Println("MySqlIngestProcessor processor ")
		data = append(data, []byte("\nProcessed by MySqlIngestProcessor processor\n")...)
		data = append(data, []byte("\t|\n")...)
		backendConn := p.GetBackendConnector(quesma_api.MySQLBackend)
		if backendConn != nil {
			fmt.Println("Backend connector found")
			err := backendConn.Open()
			if err != nil {
				fmt.Printf("Error opening connection: %v", err)
				return nil, nil, err
			}
			// Create table SQL statement
			createTableQuery := `
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`
			// Execute the query
			err = backendConn.Exec(context.Background(), createTableQuery)
			if err != nil {
				fmt.Printf("Failed to create table: %v\n", err)
				return nil, nil, err
			}
			id := uuid.New()
			username := "user" + id.String()
			email := username + "@quesma.com"
			// Execute the insert statement directly using Exec
			err = backendConn.Exec(context.Background(), "INSERT INTO users (username, email) VALUES (?, ?)", username, email)
			data = append(data, []byte(fmt.Sprintf("\tUser: ID=%s, Username=%s, Email=%s, CreatedAt=\n", id, username, email))...)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	return metadata, data, nil
}

func (p *MySqlIngestProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.MySQLBackend}
}
