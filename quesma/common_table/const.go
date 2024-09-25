// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package common_table

import (
	"database/sql"
	"quesma/logger"
)

const TableName = "quesma_common_table"
const IndexNameColumn = "__quesma_index_name"

const singleTableDDL = `
CREATE TABLE IF NOT EXISTS "quesma_common_table"
(
    "attributes_values" Map(String, String),
    "attributes_metadata" Map(String, String),

    "@timestamp"        DateTime64 DEFAULT now64(),
    "__quesma_index_name" LowCardinality(String) COMMENT 'Index name of the entry',
)
    ENGINE = MergeTree
    ORDER BY ("@timestamp","__quesma_index_name")
    COMMENT 'Quesma managed. Multiple indices are stored here.'

`

func EnsureCommonTableExists(db *sql.DB) {

	logger.Info().Msgf("Ensuring common table '%v' exists", TableName)
	_, err := db.Exec(singleTableDDL)
	if err != nil {
		// TODO check if we've got RO access to the database
		logger.Warn().Msgf("Failed to create common table '%v': %v", TableName, err)

		// maybe we should toggle some flag here
	} else {
		logger.Info().Msgf("common table '%v' created", TableName)
	}
}

// Here are defintion of JSON objects that are used to store virtual tables in JSON database

const VirtualTableElasticIndexName = "quesma_virtual_tables"
const VirtualTableStructVersion = "1"

type VirtualTableColumn struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Comment string `json:"comment"`
}

type VirtualTable struct {
	Version  string               `json:"version"`
	StoredAt string               `json:"stored_at"`
	Columns  []VirtualTableColumn `json:"columns"`
}
