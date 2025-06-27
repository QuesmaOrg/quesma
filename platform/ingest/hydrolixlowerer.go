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
)

type HydrolixLowerer struct {
	virtualTableStorage persistence.JSONDatabase
}

func NewHydrolixLowerer(virtualTableStorage persistence.JSONDatabase) *HydrolixLowerer {
	return &HydrolixLowerer{
		virtualTableStorage: virtualTableStorage,
	}
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
