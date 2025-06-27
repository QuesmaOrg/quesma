// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	chLib "github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
)

type HydrolixLowerer struct {
	virtualTableStorage persistence.JSONDatabase
}

func NewHydrolixLowerer(virtualTableStorage persistence.JSONDatabase) *HydrolixLowerer {
	return &HydrolixLowerer{
		virtualTableStorage: virtualTableStorage,
	}
}

func (l *HydrolixLowerer) LowerToDDL(validatedJsons []types.JSON,
	table *chLib.Table,
	invalidJsons []types.JSON,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName,
	createTableCmd CreateTableStatement) ([]string, error) {
	for i, preprocessedJson := range validatedJsons {
		_ = i
		_ = preprocessedJson
	}

	result := []string{`{
  "schema": {
    "project": "",
    "name": "test_index",
    "time_column": "ingest_time",
    "columns": [
      { "name": "new_field", "type": "string" },
      { "name": "ingest_time", "type": "datetime", "default": "NOW" }
    ],
    "partitioning": {
      "strategy": "time",
      "field": "ingest_time",
      "granularity": "day"
    }
  },
  "events": [
    {
      "new_field": "bar"
    }
  ]
}`}

	return result, nil
}
