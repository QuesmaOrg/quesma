// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"fmt"
	chLib "github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"strings"
	"sync/atomic"
)

type HydrolixLowerer struct {
	virtualTableStorage persistence.JSONDatabase
	ingestCounter       int64
}

func NewHydrolixLowerer(virtualTableStorage persistence.JSONDatabase) *HydrolixLowerer {
	return &HydrolixLowerer{
		virtualTableStorage: virtualTableStorage,
	}
}

func (ip *HydrolixLowerer) GenerateIngestContent(table *chLib.Table,
	data types.JSON,
	inValidJson types.JSON,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName) ([]AlterStatement, types.JSON, []NonSchemaField, error) {

	if len(table.Config.Attributes) == 0 {
		return nil, data, nil, nil
	}

	mDiff := DifferenceMap(data, table) // TODO change to DifferenceMap(m, t)

	if len(mDiff) == 0 && len(inValidJson) == 0 { // no need to modify, just insert 'js'
		return nil, data, nil, nil
	}

	// check attributes precondition
	if len(table.Config.Attributes) <= 0 {
		return nil, nil, nil, fmt.Errorf("no attributes config, but received non-schema fields: %s", mDiff)
	}
	attrsMap, _ := BuildAttrsMap(mDiff, table.Config)

	// generateNewColumns is called on original attributes map
	// before adding invalid fields to it
	// otherwise it would contain invalid fields e.g. with wrong types
	// we only want to add fields that are not part of the schema e.g we don't
	// have columns for them
	var alterStatements []AlterStatement
	atomic.AddInt64(&ip.ingestCounter, 1)
	//if ok, alteredAttributesIndexes := ip.shouldAlterColumns(table, attrsMap); ok {
	//	alterStatements = ip.generateNewColumns(attrsMap, table, alteredAttributesIndexes, encodings)
	//}
	// If there are some invalid fields, we need to add them to the attributes map
	// to not lose them and be able to store them later by
	// generating correct update query
	// addInvalidJsonFieldsToAttributes returns a new map with invalid fields added
	// this map is then used to generate non-schema fields string
	attrsMapWithInvalidFields := addInvalidJsonFieldsToAttributes(attrsMap, inValidJson)
	nonSchemaFields, err := generateNonSchemaFields(attrsMapWithInvalidFields)

	if err != nil {
		return nil, nil, nil, err
	}

	onlySchemaFields := RemoveNonSchemaFields(data, table)

	return alterStatements, onlySchemaFields, nonSchemaFields, nil
}

func (l *HydrolixLowerer) LowerToDDL(
	validatedJsons []types.JSON,
	table *chLib.Table,
	invalidJsons []types.JSON,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName,
	createTableCmd CreateTableStatement,
) ([]string, error) {
	// Construct columns array
	var columnsJSON strings.Builder
	columnsJSON.WriteString("[\n")

	for i, col := range createTableCmd.Columns {
		if i > 0 {
			columnsJSON.WriteString(",\n")
		}
		columnsJSON.WriteString(fmt.Sprintf(`  { "name": "%s", "type": "%s"`, col.ColumnName, col.ColumnType))
		if col.Comment != "" {
			columnsJSON.WriteString(fmt.Sprintf(`, "comment": "%s"`, col.Comment))
		}
		if col.AdditionalMetadata != "" {
			columnsJSON.WriteString(fmt.Sprintf(`, "metadata": "%s"`, col.AdditionalMetadata))
		}
		columnsJSON.WriteString(" }")
	}

	columnsJSON.WriteString("\n]")

	const timeColumnName = "ingest_time"

	const (
		partitioningStrategy    = "strategy"
		partitioningField       = "field"
		partitioningGranularity = "granularity"

		defaultStrategy    = "time"
		defaultField       = "ingest_time"
		defaultGranularity = "day"
	)
	partitioningJSON := fmt.Sprintf(`"partitioning": {
  "%s": "%s",
  "%s": "%s",
  "%s": "%s"
}`,
		partitioningStrategy, defaultStrategy,
		partitioningField, defaultField,
		partitioningGranularity, defaultGranularity)

	for i, preprocessedJson := range validatedJsons {
		alter, onlySchemaFields, nonSchemaFields, err := l.GenerateIngestContent(table, preprocessedJson,
			invalidJsons[i], encodings)
		_ = alter
		_ = onlySchemaFields
		_ = nonSchemaFields
		if err != nil {
			return nil, fmt.Errorf("error BuildInsertJson, tablename: '%s' : %v", table.Name, err)
		}
	}

	result := fmt.Sprintf(`{
  "schema": {
    "project": "%s",
    "name": "%s",
    "time_column": "%s",
    "columns": %s,
    %s,
  },
  "events": [
    {
      "new_field": "bar"
    }
  ]
}`, table.DatabaseName, table.Name, timeColumnName, columnsJSON.String(), partitioningJSON)

	return []string{result}, nil
}
