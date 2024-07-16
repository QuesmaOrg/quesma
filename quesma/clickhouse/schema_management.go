// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"database/sql"
	"quesma/end_user_errors"
	"quesma/logger"
)

type SchemaManagement struct {
	chDb *sql.DB
}

func NewSchemaManagement(chDb *sql.DB) *SchemaManagement {
	return &SchemaManagement{chDb: chDb}
}

func (s *SchemaManagement) readTables(database string) (map[string]map[string]string, error) {
	logger.Debug().Msgf("describing tables: %s", database)

	rows, err := s.chDb.Query("SELECT table, name, type FROM system.columns WHERE database = ? AND database != 'system'", database)
	if err != nil {
		err = end_user_errors.GuessClickhouseErrorType(err).InternalDetails("reading list of columns from system.columns")
		return map[string]map[string]string{}, err
	}
	defer rows.Close()
	columnsPerTable := make(map[string]map[string]string)
	for rows.Next() {
		var table, colName, colType string
		if err := rows.Scan(&table, &colName, &colType); err != nil {
			return map[string]map[string]string{}, err
		}
		if _, ok := columnsPerTable[table]; !ok {
			columnsPerTable[table] = make(map[string]string)
		}
		columnsPerTable[table][colName] = colType
	}

	return columnsPerTable, nil
}

func (s *SchemaManagement) tablePrimaryKey(database, table, dbKind string) (primaryKey string) {
	switch dbKind {
	case "hydrolix":
		return s.getTimestampFieldForHydrolix(database, table)
	case "clickhouse":
		return s.getTimestampFieldForClickHouse(database, table)
	default:
		return ""
	}
}

func (s *SchemaManagement) getTimestampFieldForHydrolix(database, table string) (timestampField string) {
	// In Hydrolix, there's always only one column in a table set as a primary timestamp
	// Ref: https://docs.hydrolix.io/docs/transforms-and-write-schema#primary-timestamp
	if err := s.chDb.QueryRow("SELECT primary_key FROM system.tables WHERE database = ? and table = ?", database, table).Scan(&timestampField); err != nil {
		logger.Error().Msgf("failed fetching primary key for table %s: %v", table, err)
	}
	return timestampField
}

func (s *SchemaManagement) getTimestampFieldForClickHouse(database, table string) (timestampField string) {
	// In ClickHouse, there's no concept of a primary timestamp field, primary keys are often composite,
	// hence we have to use following heuristic to determine the timestamp field (also just picking the first column if there are multiple)
	if err := s.chDb.QueryRow("SELECT name FROM system.columns WHERE database = ? AND table = ? AND is_in_primary_key = 1 AND type iLIKE 'DateTime%'", database, table).Scan(&timestampField); err != nil {
		logger.Error().Msgf("failed fetching primary key for table %s: %v", table, err)
		return
	}
	return timestampField
}

func (s *SchemaManagement) tableComment(database, table string) (comment string) {

	err := s.chDb.QueryRow("SELECT comment FROM system.tables WHERE database = ? and table = ?", database, table).Scan(&comment)

	if err != nil {
		logger.Error().Msgf("could not get table comment: %v", err)
	}
	return comment
}

func (s *SchemaManagement) createTableQuery(database, table string) (ddl string) {
	err := s.chDb.QueryRow("SELECT create_table_query FROM system.tables WHERE database = ? and table = ? ", database, table).Scan(&ddl)

	if err != nil {
		logger.Error().Msgf("could not get table comment: %v", err)
	}
	return ddl
}
