// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "github.com/QuesmaOrg/quesma/platform/model"

// Tests for numeric facets (int64, float64).
// Tests for string facets are already covered in "standard" queries (see testdata/requests.go, testdata/aggregation_requests.go),
// so not repeating them here.
var TestsNumericFacets = []struct {
	Name          string
	QueryJson     string
	ResultJson    string
	ExpectedSql   string
	NewResultRows []model.QueryResultRow
}{
	{
		Name: "facets, int64 as key, 3 (<10) values",
		QueryJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"max_value": {
							"max": {
								"field": "int64-field"
							}
						},
						"min_value": {
							"min": {
								"field": "int64-field"
							}
						},
						"sample_count": {
							"value_count": {
								"field": "int64-field"
							}
						},
						"top_values": {
							"terms": {
								"field": "int64-field",
								"size": 10
							}
						}
					},
					"sampler": {
						"shard_size": 5000
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"bool": {
								"filter": [],
								"must": [],
								"must_not": [],
								"should": []
							}
						}
					]
				}
			},
			"runtime_mappings": {
				"hour_of_day": {
					"script": {
						"source": "emit(doc['timestamp'].value.getHour());"
					},
					"type": "long"
				}
			},
			"size": 0,
			"track_total_hits": true
		}`,
		ResultJson: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_19",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 0,
					"total": 0
				},
				"aggregations": {
					"sample": {
						"doc_count": 2693,
						"max_value": {
							"value": 12140.860228566502
						},
						"min_value": {
							"value": 0
						},
						"sample_count": {
							"value": 2693
						},
						"top_values": {
                            "sum_other_doc_count": 2567,
							"buckets": [
								{
									"doc_count": 121,
									"key": 0
								},
								{
									"doc_count": 3,
									"key": 12.490584583112518
								},
								{
									"doc_count": 2,
									"key": 26.07052481248436
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": 0,
					"total": {
						"relation": "eq",
						"value": 2693
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedSql: "SELECT sum(count(*)) OVER () AS `metric____quesma_total_count_col_0`,\n" +
			"  sum(count(*)) OVER () AS `aggr__sample__count`,\n" +
			"  maxOrNull(maxOrNull(`int64-field`)) OVER () AS\n" +
			"  `metric__sample__max_value_col_0`,\n" +
			"  minOrNull(minOrNull(`int64-field`)) OVER () AS\n" +
			"  `metric__sample__min_value_col_0`,\n" +
			"  sum(count(`int64-field`)) OVER () AS `metric__sample__sample_count_col_0`,\n" +
			"  sum(count(*)) OVER () AS `aggr__sample__top_values__parent_count`,\n" +
			"  `int64-field` AS `aggr__sample__top_values__key_0`,\n" +
			"  count(*) AS `aggr__sample__top_values__count`\n" +
			"FROM (\n" +
			"  SELECT `int64-field`\n" +
			"  FROM `__quesma_table_name`\n" +
			"  LIMIT 20000)\n" +
			"GROUP BY `int64-field` AS `aggr__sample__top_values__key_0`\n" +
			"ORDER BY `aggr__sample__top_values__count` DESC,\n" +
			"  `aggr__sample__top_values__key_0` ASC\n" +
			"LIMIT 11",
		NewResultRows: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric____quesma_total_count_col_0", 2693),
				model.NewQueryResultCol("aggr__sample__count", 2693),
				model.NewQueryResultCol("metric__sample__max_value_col_0", 12140.860228566502),
				model.NewQueryResultCol("metric__sample__min_value_col_0", 0),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", 0),
				model.NewQueryResultCol("aggr__sample__top_values__count", 121),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric____quesma_total_count_col_0", 2693),
				model.NewQueryResultCol("aggr__sample__count", 2693),
				model.NewQueryResultCol("metric__sample__max_value_col_0", 12140.860228566502),
				model.NewQueryResultCol("metric__sample__min_value_col_0", 0),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", 12.490585),
				model.NewQueryResultCol("aggr__sample__top_values__count", 3),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric____quesma_total_count_col_0", 2693),
				model.NewQueryResultCol("aggr__sample__count", 2693),
				model.NewQueryResultCol("metric__sample__max_value_col_0", 12140.860228566502),
				model.NewQueryResultCol("metric__sample__min_value_col_0", 0),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", 26.070525),
				model.NewQueryResultCol("aggr__sample__top_values__count", 2),
			}},
		},
	},
}
