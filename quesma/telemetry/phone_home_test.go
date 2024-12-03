// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package telemetry

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPhoneHome_ParseElastic(t *testing.T) {

	responseAsJson := `
{
  "_shards" : {
    "total" : 12,
    "successful" : 6,
    "failed" : 0
  },
  "_all" : {
    "primaries" : {
      "docs" : {
        "count" : 74874,
        "deleted" : 0
      },
      "shard_stats" : {
        "total_count" : 6
      },
      "store" : {
        "size_in_bytes" : 28346964,
        "total_data_set_size_in_bytes" : 28346964,
        "reserved_in_bytes" : 0
      },
      "indexing" : {
        "index_total" : 74907,
        "index_time_in_millis" : 2515,
        "index_current" : 0,
        "index_failed" : 0,
        "delete_total" : 0,
        "delete_time_in_millis" : 0,
        "delete_current" : 0,
        "noop_update_total" : 0,
        "is_throttled" : false,
        "throttle_time_in_millis" : 0,
        "write_load" : 0.0017084311790057697
      },
      "get" : {
        "total" : 0,
        "time_in_millis" : 0,
        "exists_total" : 0,
        "exists_time_in_millis" : 0,
        "missing_total" : 0,
        "missing_time_in_millis" : 0,
        "current" : 0
      },
      "search" : {
        "open_contexts" : 0,
        "query_total" : 0,
        "query_time_in_millis" : 0,
        "query_current" : 0,
        "fetch_total" : 0,
        "fetch_time_in_millis" : 0,
        "fetch_current" : 0,
        "scroll_total" : 0,
        "scroll_time_in_millis" : 0,
        "scroll_current" : 0,
        "suggest_total" : 0,
        "suggest_time_in_millis" : 0,
        "suggest_current" : 0
      },
      "merges" : {
        "current" : 0,
        "current_docs" : 0,
        "current_size_in_bytes" : 0,
        "total" : 2,
        "total_time_in_millis" : 53,
        "total_docs" : 186,
        "total_size_in_bytes" : 1004292,
        "total_stopped_time_in_millis" : 0,
        "total_throttled_time_in_millis" : 0,
        "total_auto_throttle_in_bytes" : 125829120
      },
      "refresh" : {
        "total" : 100,
        "total_time_in_millis" : 1442,
        "external_total" : 88,
        "external_total_time_in_millis" : 1554,
        "listeners" : 0
      },
      "flush" : {
        "total" : 12,
        "periodic" : 12,
        "total_time_in_millis" : 361
      },
      "warmer" : {
        "current" : 0,
        "total" : 82,
        "total_time_in_millis" : 88
      },
      "query_cache" : {
        "memory_size_in_bytes" : 0,
        "total_count" : 0,
        "hit_count" : 0,
        "miss_count" : 0,
        "cache_size" : 0,
        "cache_count" : 0,
        "evictions" : 0
      },
      "fielddata" : {
        "memory_size_in_bytes" : 0,
        "evictions" : 0,
        "global_ordinals" : {
          "build_time_in_millis" : 0
        }
      },
      "completion" : {
        "size_in_bytes" : 0
      },
      "segments" : {
        "count" : 43,
        "memory_in_bytes" : 0,
        "terms_memory_in_bytes" : 0,
        "stored_fields_memory_in_bytes" : 0,
        "term_vectors_memory_in_bytes" : 0,
        "norms_memory_in_bytes" : 0,
        "points_memory_in_bytes" : 0,
        "doc_values_memory_in_bytes" : 0,
        "index_writer_memory_in_bytes" : 647304,
        "version_map_memory_in_bytes" : 0,
        "fixed_bit_set_memory_in_bytes" : 112,
        "max_unsafe_auto_id_timestamp" : -1,
        "file_sizes" : { }
      },
      "translog" : {
        "operations" : 31798,
        "size_in_bytes" : 34322857,
        "uncommitted_operations" : 31798,
        "uncommitted_size_in_bytes" : 34322857,
        "earliest_last_modified_age" : 1693
      },
      "request_cache" : {
        "memory_size_in_bytes" : 0,
        "evictions" : 0,
        "hit_count" : 0,
        "miss_count" : 0
      },
      "recovery" : {
        "current_as_source" : 0,
        "current_as_target" : 0,
        "throttle_time_in_millis" : 0
      },
      "bulk" : {
        "total_operations" : 1195,
        "total_time_in_millis" : 3083,
        "total_size_in_bytes" : 65774063,
        "avg_time_in_millis" : 1,
        "avg_size_in_bytes" : 25375
      },
      "dense_vector" : {
        "value_count" : 0
      }
    },
    "total" : {
      "docs" : {
        "count" : 74874,
        "deleted" : 0
      },
      "shard_stats" : {
        "total_count" : 6
      },
      "store" : {
        "size_in_bytes" : 28346964,
        "total_data_set_size_in_bytes" : 28346964,
        "reserved_in_bytes" : 0
      },
      "indexing" : {
        "index_total" : 74907,
        "index_time_in_millis" : 2515,
        "index_current" : 0,
        "index_failed" : 0,
        "delete_total" : 0,
        "delete_time_in_millis" : 0,
        "delete_current" : 0,
        "noop_update_total" : 0,
        "is_throttled" : false,
        "throttle_time_in_millis" : 0,
        "write_load" : 0.0017084311790057697
      },
      "get" : {
        "total" : 0,
        "time_in_millis" : 0,
        "exists_total" : 0,
        "exists_time_in_millis" : 0,
        "missing_total" : 0,
        "missing_time_in_millis" : 0,
        "current" : 0
      },
      "search" : {
        "open_contexts" : 0,
        "query_total" : 0,
        "query_time_in_millis" : 0,
        "query_current" : 0,
        "fetch_total" : 0,
        "fetch_time_in_millis" : 0,
        "fetch_current" : 0,
        "scroll_total" : 0,
        "scroll_time_in_millis" : 0,
        "scroll_current" : 0,
        "suggest_total" : 0,
        "suggest_time_in_millis" : 0,
        "suggest_current" : 0
      },
      "merges" : {
        "current" : 0,
        "current_docs" : 0,
        "current_size_in_bytes" : 0,
        "total" : 2,
        "total_time_in_millis" : 53,
        "total_docs" : 186,
        "total_size_in_bytes" : 1004292,
        "total_stopped_time_in_millis" : 0,
        "total_throttled_time_in_millis" : 0,
        "total_auto_throttle_in_bytes" : 125829120
      },
      "refresh" : {
        "total" : 100,
        "total_time_in_millis" : 1442,
        "external_total" : 88,
        "external_total_time_in_millis" : 1554,
        "listeners" : 0
      },
      "flush" : {
        "total" : 12,
        "periodic" : 12,
        "total_time_in_millis" : 361
      },
      "warmer" : {
        "current" : 0,
        "total" : 82,
        "total_time_in_millis" : 88
      },
      "query_cache" : {
        "memory_size_in_bytes" : 0,
        "total_count" : 0,
        "hit_count" : 0,
        "miss_count" : 0,
        "cache_size" : 0,
        "cache_count" : 0,
        "evictions" : 0
      },
      "fielddata" : {
        "memory_size_in_bytes" : 0,
        "evictions" : 0,
        "global_ordinals" : {
          "build_time_in_millis" : 0
        }
      },
      "completion" : {
        "size_in_bytes" : 0
      },
      "segments" : {
        "count" : 43,
        "memory_in_bytes" : 0,
        "terms_memory_in_bytes" : 0,
        "stored_fields_memory_in_bytes" : 0,
        "term_vectors_memory_in_bytes" : 0,
        "norms_memory_in_bytes" : 0,
        "points_memory_in_bytes" : 0,
        "doc_values_memory_in_bytes" : 0,
        "index_writer_memory_in_bytes" : 647304,
        "version_map_memory_in_bytes" : 0,
        "fixed_bit_set_memory_in_bytes" : 112,
        "max_unsafe_auto_id_timestamp" : -1,
        "file_sizes" : { }
      },
      "translog" : {
        "operations" : 31798,
        "size_in_bytes" : 34322857,
        "uncommitted_operations" : 31798,
        "uncommitted_size_in_bytes" : 34322857,
        "earliest_last_modified_age" : 1693
      },
      "request_cache" : {
        "memory_size_in_bytes" : 0,
        "evictions" : 0,
        "hit_count" : 0,
        "miss_count" : 0
      },
      "recovery" : {
        "current_as_source" : 0,
        "current_as_target" : 0,
        "throttle_time_in_millis" : 0
      },
      "bulk" : {
        "total_operations" : 1195,
        "total_time_in_millis" : 3083,
        "total_size_in_bytes" : 65774063,
        "avg_time_in_millis" : 1,
        "avg_size_in_bytes" : 25375
      },
      "dense_vector" : {
        "value_count" : 0
      }
    }
  },
  "indices" : {
  }
}


`

	response := &elasticStatsResponse{}
	err := json.Unmarshal([]byte(responseAsJson), response)

	assert.Nil(t, err)

	assert.Equal(t, int64(74874), response.All.Total.Docs.Count)
	assert.Equal(t, int64(28346964), response.All.Total.Store.SizeInBytes)

}

func TestAgent_CollectElastic_Version(t *testing.T) {

	// $ curl localhost:9200
	responseAsJson := `

{
  "name" : "a64526973c4d",
  "cluster_name" : "docker-cluster",
  "cluster_uuid" : "3tIKdAuNRS2OAb4LQPREmw",
  "version" : {
    "number" : "8.11.1",
    "build_flavor" : "default",
    "build_type" : "docker",
    "build_hash" : "6f9ff581fbcde658e6f69d6ce03050f060d1fd0c",
    "build_date" : "2023-11-11T10:05:59.421038163Z",
    "build_snapshot" : false,
    "lucene_version" : "9.8.0",
    "minimum_wire_compatibility_version" : "7.17.0",
    "minimum_index_compatibility_version" : "7.0.0"
  },
  "tagline" : "You Know, for Search"
}
`

	response := &elasticVersionResponse{}
	err := json.Unmarshal([]byte(responseAsJson), response)

	assert.Nil(t, err)

	assert.Equal(t, "8.11.1", response.Version.Number)

}

func TestGetTopNValues(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]int64
		n        int
		expected map[string]int64
	}{
		{
			name: "LessThanN",
			input: map[string]int64{
				"table2": 300,
				"table1": 500,
			},
			n: 5,
			expected: map[string]int64{
				"table1": 500,
				"table2": 300,
			},
		},
		{
			name: "EqualToN",
			input: map[string]int64{
				"table1": 200,
				"table3": 500,
				"table2": 300,
			},
			n: 3,
			expected: map[string]int64{
				"table3": 500,
				"table2": 300,
				"table1": 200,
			},
		},
		{
			name: "MoreThanN",
			input: map[string]int64{
				"table2": 300,
				"table4": 100,
				"table1": 500,
				"table3": 200,
			},
			n: 3,
			expected: map[string]int64{
				"table1": 500,
				"table2": 300,
				"table3": 200,
			},
		},
		{
			name:     "EmptyMap",
			input:    map[string]int64{},
			n:        3,
			expected: map[string]int64{},
		},
		{
			name: "NegativeN",
			input: map[string]int64{
				"table1": 500,
				"table2": 300,
			},
			n:        -1,
			expected: map[string]int64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getTopNValues(tt.input, tt.n)
			assert.Equal(t, tt.expected, result)
		})
	}
}
