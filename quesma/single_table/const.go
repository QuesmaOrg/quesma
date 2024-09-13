// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package single_table

import (
	"database/sql"
	"quesma/logger"
)

const TableName = "quesma_all_logs"
const IndexNameColumn = "__quesma_index_name"

const singleTableDDL = `
CREATE TABLE IF NOT EXISTS "quesma_all_logs"
(
    "attributes_values" Map(String, String),
    "attributes_metadata" Map(String, String),

    "@timestamp"        DateTime64 DEFAULT now64(),
    "__quesma_index_name" LowCardinality(String) COMMENT 'Index name of the entry',
)
    ENGINE = MergeTree
    ORDER BY ("@timestamp","__quesma_index_name")
    COMMENT 'Quesma managed. Multiple logs are stored here.'

`

func EnsureSingleTableExists(db *sql.DB) {

	logger.Info().Msgf("Ensuring single table '%v' exists", TableName)
	_, err := db.Exec(singleTableDDL)
	if err != nil {
		// TODO check if we've got RO access to the database
		logger.Warn().Msgf("Failed to create single table '%v': %v", TableName, err)

		// maybe we should toggle some flag here
	} else {
		logger.Info().Msgf("Single table '%v' created", TableName)
	}
}

// Here are defintion of JSON objects that are used to store virtual tables in JSON database

type VirtualTableColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type VirtualTable struct {
	StoredAt string `json:"stored_at"`
	Columns  string `json:"columns"` // here we keep columns as a JSON string, we don't want to exceed limit of fields in
}
