// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package ingest

import (
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJsonToColumns(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		expected []CreateTableEntry
	}{
		{
			name:    "NestedTuples",
			payload: `{"nested_tuples":[[{"test":{"id":"asdf","ns":"rrrr"}}]]}`,
			expected: []CreateTableEntry{
				{ClickHouseColumnName: "nested_tuples", ClickHouseType: "Array(Array(Tuple(test Tuple(id Nullable(String), ns Nullable(String)))))"},
			},
		},
		{
			name:    "NotSoDeeplyNested",
			payload: `{"not_so_deeply_nested":[{"test":{"id":0.1337,"ns":1233}}]}`,
			expected: []CreateTableEntry{
				{ClickHouseColumnName: "not_so_deeply_nested", ClickHouseType: "Array(Tuple(test Tuple(id Nullable(Float64), ns Nullable(Int64))))"},
			},
		},
		{
			name:    "Timestamp",
			payload: `{"@timestamp":"2024-01-27T16:11:19.94Z"}`,
			expected: []CreateTableEntry{
				{ClickHouseColumnName: "@timestamp", ClickHouseType: "DateTime64 DEFAULT now64()"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			json, _ := types.ParseJSON(tt.payload)

			result := JsonToColumns(json, &clickhouse.ChTableConfig{
				TimestampDefaultsNow: true,
			})

			assert.Equal(t, tt.expected, result)
		})
	}
}
