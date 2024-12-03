// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"github.com/rs/zerolog/log"
	"quesma/quesma/types"
	"quesma_v2/core"
	"quesma_v2/processors"
)

const (
	IngestAction    = "ingest_action"
	DocIndexAction  = "_doc"
	BulkIndexAction = "_bulk"
)

type ElasticsearchToClickHouseIngestProcessor struct {
	processors.BaseProcessor
}

func NewElasticsearchToClickHouseIngestProcessor() *ElasticsearchToClickHouseIngestProcessor {
	return &ElasticsearchToClickHouseIngestProcessor{
		BaseProcessor: processors.NewBaseProcessor(),
	}
}

func (p *ElasticsearchToClickHouseIngestProcessor) GetId() string {
	return "elasticsearch_to_clickhouse_ingest"
}

func (p *ElasticsearchToClickHouseIngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	// TODO this processor should NOT take multiple messages? :|

	for _, m := range message {
		bodyAsBytes, err := quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("ElasticsearchToClickHouseIngestProcessor: invalid message type")
		}
		targetIndex := "my_index" // TODO: remove this ASAP
		// side-effecting for now - just store in ClickHouse it's fine for now
		switch metadata[IngestAction] {
		case DocIndexAction:
			payloadJson, _ := types.ExpectJSON(bodyAsBytes)
			handleDocIndex(payloadJson, targetIndex)
			println("DocIndexAction")
		case BulkIndexAction:
			payloadNDJson, _ := types.ExpectNDJSON(bodyAsBytes)
			handleBulkIndex(payloadNDJson, targetIndex)
			println("BulkIndexAction")
		default:
			log.Info().Msg("Rethink you whole life and start over again")
		}
		//		if err != nil {
		//			panic("ElasticsearchToClickHouseQueryProcessor: invalid message type")
		//		}
		//		data = mCasted
		//		fmt.Println("ElasticsearchToClickHouseIngest processor ")
		//		data = append(data, []byte("\nProcessed by ElasticsearchToClickHouseIngest processor\n")...)
		//		data = append(data, []byte("\t|\n")...)
		//		backendConn := p.GetBackendConnector(quesma_api.ClickHouseSQLBackend)
		//		if backendConn == nil {
		//			fmt.Println("Backend connector not found")
		//			return metadata, data, nil
		//		}
		//
		//		err = backendConn.Open()
		//		if err != nil {
		//			fmt.Printf("Error opening connection: %v", err)
		//			return nil, nil, err
		//		}
		//		createTableSQL := `
		//CREATE TABLE IF NOT EXISTS users (
		//    id UInt32,                              -- ClickHouse doesn't have SERIAL, use UInt32 or UInt64 for auto-increment.
		//    username String,                        -- ClickHouse uses String for variable-length text.
		//    email String,                           -- No UNIQUE constraint, but String type works for emails.
		//    created_at DateTime DEFAULT now()       --
		//)
		//ENGINE = MergeTree()                        -- MergeTree is the common engine in ClickHouse.
		//ORDER BY (id);                              -- ClickHouse requires an ORDER BY clause for the MergeTree engine.
		//    `
		//
		//		err = backendConn.Exec(context.Background(), createTableSQL)
		//		if err != nil {
		//			log.Fatalf("Failed to create table: %v\n", err)
		//		}
		//
		//		id := uuid.New()
		//		username := "user" + id.String()
		//		email := username + "@quesma.com"
		//
		//		// Insert data into the users table
		//		insertSQL := `INSERT INTO users (username, email) VALUES ($1, $2)`
		//
		//		err = backendConn.Exec(context.Background(), insertSQL, username, email)
		//		if err != nil {
		//			fmt.Printf("Error inserting data: %v", err)
		//			return nil, nil, err
		//		}
		//		data = append(data, []byte(fmt.Sprintf("\tUser: ID=%s, Username=%s, Email=%s, CreatedAt=\n", id, username, email))...)
		//		err = backendConn.Close()
		//		if err != nil {
		//			fmt.Printf("Error closing connection: %v", err)
		//			return nil, nil, err
		//		}
	}
	return metadata, data, nil
}

func (p *ElasticsearchToClickHouseIngestProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.ClickHouseSQLBackend}
}
