// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "quesma/model"

var AggregationTestsWithDates = []AggregationTestCase{
	{ // [0]
		TestName: "simple max/min aggregation as 2 siblings",
		QueryRequestJson: `
		{
			"aggs": {
				"sampler": {
					"aggs": {
						"eventRate": {
							"date_histogram": {
								"extended_bounds": {
									"max": 1727859403270,
									"min": 1727858503270
								},
								"field": "order_date",
								"calendar_interval": "1w",
								"min_doc_count": 0
							}
						}
					},
					"random_sampler": {
						"probability": 0.000001,
						"seed": "1292529172"
					}
				}
			},
			"size": 0,
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436398,
			"expiration_time_in_millis": 1707486496397,
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"sampler": {
						"doc_count": 4675,
						"eventRate": {
							"buckets": [
								{
									"doc_count": 442,
									"key": 1726358400000,
									"key_as_string": "2024-09-15T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1726963200000,
									"key_as_string": "2024-09-22T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1727568000000,
									"key_as_string": "2024-09-29T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1728172800000,
									"key_as_string": "2024-10-06T00:00:00.000"
								},
								{
									"doc_count": 1,
									"key": 1728777600000,
									"key_as_string": "2024-10-13T00:00:00.000"
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2200
					}
				},
				"timed_out": false,
				"took": 1
			},
			"start_time_in_millis": 1707486436397
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", int64(4675)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1726358400000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", int64(442)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", int64(4675)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1728777600000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sampler__count",
			  toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("order_date", 'UTC'))))*1000
			  AS "aggr__sampler__eventRate__key_0",
			  count(*) AS "aggr__sampler__eventRate__count"
			FROM (
			  SELECT "order_date"
			  FROM __quesma_table_name
			  LIMIT 20000)
			GROUP BY toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("order_date", 'UTC')))
			  )*1000 AS "aggr__sampler__eventRate__key_0"
			ORDER BY "aggr__sampler__eventRate__key_0" ASC`,
	},
	{ // [1]
		TestName: "multiple buckets_path",
		QueryRequestJson: `
		{
			"aggs": {
				"timeseries": {
					"aggs": {
						"c4a48962-08e6-4791-ae9e-b50f1b111488": {
							"bucket_script": {
								"buckets_path": {
									"count": "_count"
								},
								"gap_policy": "skip",
								"script": {
									"lang": "expression",
									"source": "count * 1"
								}
							}
						}
					},
					"date_histogram": {
						"extended_bounds": {
							"max": 1730370516174,
							"min": 1730369696174
						},
						"field": "@timestamp",
						"fixed_interval": "10s",
						"min_doc_count": 0,
						"time_zone": "Europe/Warsaw"
					},
					"meta": {
						"dataViewId": "logs-generic",
						"indexPatternString": "logs-generic-*",
						"intervalString": "10s",
						"normalized": true,
						"panelId": "b8a0f82e-3186-4e4c-9b77-2092922d38e6",
						"seriesId": "0eb0b521-7833-495a-b99b-877b01eb0513",
						"timeField": "@timestamp"
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"took": 0,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"failed": 0,
				"skipped": 0
			},
			"hits": {
				"total": {
					"value": 26,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"__quesma_total_count": {},
				"timeseries": {
					"buckets": [
						{
							"__quesma_originalKey": 173036968,
							"doc_count": 0,
							"key": 1730366080000,
							"key_as_string": "2024-10-31T09:14:40.000"
						},
						{
							"__quesma_originalKey": 173036969,
							"doc_count": 1,
							"key": 1730366090000,
							"key_as_string": "2024-10-31T09:14:50.000", 
							"c4a48962-08e6-4791-ae9e-b50f1b111488": {
								"value": 1
							}
						}
					],
					"meta": {
						"dataViewId": "logs-generic",
						"indexPatternString": "logs-generic-*",
						"intervalString": "10s",
						"normalized": true,
						"panelId": "b8a0f82e-3186-4e4c-9b77-2092922d38e6",
						"seriesId": "0eb0b521-7833-495a-b99b-877b01eb0513",
						"timeField": "@timestamp"
					}
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730369690000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0",
			  count(*) AS "aggr__timeseries__count"
			FROM __quesma_table_name
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0"
			ORDER BY "aggr__timeseries__key_0" ASC`,
	},
}
