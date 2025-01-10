// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package stats

import (
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStatistics_process(t *testing.T) {
	stats := New()

	var json1 = map[string]interface{}{
		"a": "value1",
		"key1": map[string]interface{}{
			"foo1": "bar1",
			"foo2": "bar2",
		},
	}

	var json2 = map[string]interface{}{
		"a": "value2",
		"key2": map[string]interface{}{
			"foo1": "bar2",
		},
	}

	var json3 = map[string]interface{}{
		"b": "value1",
		"key2": map[string]interface{}{
			"foo2": "bar2",
			"foo3": "bar3",
			"foo4": "42",
			"foo5": "true",
			"foo6": "2024-01-29T09:11:24.349Z",
		},
	}

	marshal1, _ := json.Marshal(json1)
	marshal2, _ := json.Marshal(json2)
	marshal3, _ := json.Marshal(json3)
	stats.Process(true, "index1", types.MustJSON(string(marshal1)), "::")
	stats.Process(true, "index1", types.MustJSON(string(marshal2)), "::")
	stats.Process(true, "index1", types.MustJSON(string(marshal3)), "::")
	stats.Process(true, "index1", types.MustJSON(string(marshal3)), "::")

	ingestStats := (*stats)["index1"]

	assert.Equal(t, 1, len(*stats))
	assert.Equal(t, int64(4), ingestStats.Requests)
	assert.Equal(t, int64(1), ingestStats.Keys["key2::foo1"].Occurrences)
	assert.Equal(t, int64(1), ingestStats.Keys["key2::foo1"].Values["bar2"].Occurrences)
	assert.Equal(t, int64(2), ingestStats.Keys["a"].Occurrences)
	assert.Equal(t, int64(1), ingestStats.Keys["a"].Values["value1"].Occurrences)
	assert.Equal(t, int64(1), ingestStats.Keys["a"].Values["value2"].Occurrences)
	assert.Equal(t, int64(2), ingestStats.Keys["b"].Occurrences)
	assert.Equal(t, int64(2), ingestStats.Keys["b"].Values["value1"].Occurrences)
	assert.Equal(t, int64(2), ingestStats.Keys["key2::foo2"].Occurrences)
	assert.Equal(t, int64(2), ingestStats.Keys["key2::foo2"].Values["bar2"].Occurrences)
	assert.Equal(t, int64(2), ingestStats.Keys["key2::foo3"].Values["bar3"].Occurrences)
	assert.Equal(t, int64(2), ingestStats.Keys["key2::foo4"].Values["42"].Occurrences)
	assert.Contains(t, ingestStats.Keys["key2::foo4"].Values["42"].Types, "int")
	assert.Contains(t, ingestStats.Keys["key2::foo4"].Values["42"].Types, "float")
	assert.Contains(t, ingestStats.Keys["key2::foo4"].Values["42"].Types, "string")
	assert.Contains(t, ingestStats.Keys["key2::foo5"].Values["true"].Types, "bool")
	assert.Contains(t, ingestStats.Keys["key2::foo5"].Values["true"].Types, "string")
	assert.Contains(t, ingestStats.Keys["key2::foo6"].Values["2024-01-29T09:11:24.349Z"].Types, "date")
	assert.Contains(t, ingestStats.Keys["key2::foo6"].Values["2024-01-29T09:11:24.349Z"].Types, "string")
	assert.Equal(t, int64(1), ingestStats.Keys["key1::foo1"].Occurrences)
	assert.Equal(t, int64(1), ingestStats.Keys["key1::foo1"].Values["bar1"].Occurrences)
}
