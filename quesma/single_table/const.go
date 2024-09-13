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
    "attributes_string_key"   Array(String),
    "attributes_string_value" Array(String),

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
