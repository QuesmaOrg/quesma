// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package ingest

import (
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJsonToColumns(t *testing.T) {
	tests := []struct {
		name               string
		payload            string
		expectedColumnName string
		expectedTypeString []string // Tuple elements order is non-deterministic
	}{
		{
			name:    "NestedTuples",
			payload: `{"nested_tuples":[[{"test":{"id":"asdf","ns":"rrrr"}}]]}`,

			expectedColumnName: "nested_tuples",
			expectedTypeString: []string{"Array(Array(Tuple(test Tuple(id Nullable(String), ns Nullable(String)))))",
				"Array(Array(Tuple(test Tuple(ns Nullable(String), id Nullable(String)))))"},
		},
		{
			name:    "NotSoDeeplyNested",
			payload: `{"not_so_deeply_nested":[{"test":{"id":0.1337,"ns":1233}}]}`,

			expectedColumnName: "not_so_deeply_nested",
			expectedTypeString: []string{"Array(Tuple(test Tuple(id Nullable(Float64), ns Nullable(Int64))))",
				"Array(Tuple(test Tuple(ns Nullable(Int64), id Nullable(Float64))))"},
		},
		{
			name:               "Timestamp",
			payload:            `{"@timestamp":"2024-01-27T16:11:19.94Z"}`,
			expectedColumnName: "@timestamp",
			expectedTypeString: []string{"DateTime64 DEFAULT now64()"},
		},
	}

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			json, _ := types.ParseJSON(tt.payload)

			result := JsonToColumns(json, &clickhouse.ChTableConfig{
				TimestampDefaultsNow: true,
			})

			assert.Equal(t, tt.expectedColumnName, result[0].ClickHouseColumnName)
			assert.Contains(t, tt.expectedTypeString, result[0].ClickHouseType)
		})
	}
}
