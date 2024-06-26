// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// AggregationQueryTypes is a list of all aggregation types in Elasticsearch.
// More details: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html
var AggregationQueryTypes = []string{
	// metrics:
	"avg",
	"boxplot",
	"cardinality",
	"extended_stats",
	"geo_bounds",
	"geo_centroid",
	"geo_line",
	"cartesian_bounds",
	"cartesian_centroid",
	"matrix_stats",
	"max",
	"median_absolute_deviation",
	"min",
	"percentile_ranks",
	"percentiles",
	"rate",
	"scripted_metric",
	"stats",
	"string_stats",
	"sum",
	"t_test",
	"top_hits",
	"top_metrics",
	"value_count",
	"weighted_avg",

	// bucket:
	"adjacency_matrix",
	"auto_date_histogram",
	"categorize_text",
	"children",
	"composite",
	"date_histogram",
	"date_range",
	"diversified_sampler",
	"filter",
	"filters",
	"frequent_item_sets",
	"geo_distance",
	"geohash_grid",
	"geohex_grid",
	"geotile_grid",
	"global",
	"histogram",
	"ip_prefix",
	"ip_range",
	"missing",
	"multi_terms",
	"nested",
	"parent",
	"random_sampler",
	"range",
	"rare_terms",
	"reverse_nested",
	"sampler",
	"significant_terms",
	"significant_text",
	"terms",
	"time_series",
	"variable_width_histogram",

	// pipeline:
	"avg_bucket",
	"bucket_script",
	"bucket_count_ks_test",
	"bucket_correlation",
	"bucket_selector",
	"bucket_sort",
	"change_point",
	"cumulative_cardinality",
	"cumulative_sum",
	"derivative",
	"extended_stats_bucket",
	"inference",
	"max_bucket",
	"min_bucket",
	"moving_avg",
	"moving_fn",
	"moving_percentiles",
	"normalize",
	"percentiles_bucket",
	"serial_diff",
	"stats_bucket",
	"sum_bucket",
}

// QueryDSLTypes is a list of all Query DSL types in Elasticsearch.
// More details: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
var QueryDSLTypes = []string{
	// Compound queries:
	"bool",
	"boosting",
	"constant_score",
	"dis_max",
	"function_score",
	// Full text queries:
	"intervals",
	"match",
	"match_bool_prefix",
	"match_phrase",
	"match_phrase_prefix",
	"combined_fields",
	"multi_match",
	"query_string",
	"simple_query_string",
	// Geo queries
	"geo_bounding_box",
	// "geo_distance", // same name as aggregation. Easier to have it commented out for now, and catch both cases in one way.
	"geo_grid",
	"geo_polygon",
	"geo_shape",
	// Shape
	"shape",
	// Joining queries
	"nested",
	"has_child",
	"has_parent",
	"parent_id",
	// Match all
	"match_all",
	// Span queries
	"span_containing",
	"span_field_masking",
	"span_first",
	"span_multi",
	"span_near",
	"span_not",
	"span_or",
	"span_term",
	"span_within",
	// Specialized queries
	"distance_feature",
	"more_like_this",
	"percolate",
	"knn",
	"rank_feature",
	"script",
	"script_score",
	"wrapper",
	"pinned",
	"rule_query",
	"weighted_tokens",
	// Term-level queries
	"exists",
	"fuzzy",
	"ids",
	"prefix",
	"range",
	"regexp",
	"term",
	"terms",
	"terms_set",
	"wildcard",
	// Text expansion
	"text_expansion",
}

// AllQueryTypes is a list of all query types in Elasticsearch.
// So far used for listing types of queries we received, but don't support.
var AllQueryTypes = append(AggregationQueryTypes, QueryDSLTypes...)
