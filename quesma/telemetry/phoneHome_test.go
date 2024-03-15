package telemetry

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPhoneHome_ParseElastic(t *testing.T) {

	json := `
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
	stats := ElasticStats{}
	err := scanElasticResponse([]byte(json), &stats)

	assert.Nil(t, err)

	assert.Equal(t, int64(74874), stats.NumberOfDocs)
	assert.Equal(t, int64(28346964), stats.Size)

}
