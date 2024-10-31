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
		TestName: "extended_bounds pre keys (timezone calculations most tricky to get right)",
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
							"min": 1730370416174
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
			"query": {
				"bool": {
					"filter": [],
					"must": [
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-10-31T10:24:56.174Z",
									"lte": "2024-10-31T10:29:56.174Z"
								}
							}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1730384367349,
			"expiration_time_in_millis": 1730816367347,
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
					"timeseries": {
						"buckets": [
							{
								"doc_count": 0,
								"key": 1730370410000,
								"key_as_string": "2024-10-31T10:26:50.000"
							},
							{
								"doc_count": 0,
								"key": 1730370420000,
								"key_as_string": "2024-10-31T10:27:00.000"
							},
							{
								"doc_count": 0,
								"key": 1730370430000,
								"key_as_string": "2024-10-31T10:27:10.000"
							},
							{
								"doc_count": 0,
								"key": 1730370440000,
								"key_as_string": "2024-10-31T10:27:20.000"
							},
							{
								"doc_count": 0,
								"key": 1730370450000,
								"key_as_string": "2024-10-31T10:27:30.000"
							},
							{
								"c4a48962-08e6-4791-ae9e-b50f1b111488": {
									"value": 1.0
								},
								"doc_count": 1,
								"key": 1730370460000,
								"key_as_string": "2024-10-31T10:27:40.000"
							},
							{
								"doc_count": 0,
								"key": 1730370470000,
								"key_as_string": "2024-10-31T10:27:50.000"
							},
							{
								"doc_count": 0,
								"key": 1730370480000,
								"key_as_string": "2024-10-31T10:28:00.000"
							},
							{
								"doc_count": 0,
								"key": 1730370490000,
								"key_as_string": "2024-10-31T10:28:10.000"
							},
							{
								"doc_count": 0,
								"key": 1730370500000,
								"key_as_string": "2024-10-31T10:28:20.000"
							},
							{
								"c4a48962-08e6-4791-ae9e-b50f1b111488": {
									"value": 1.0
								},
								"doc_count": 1,
								"key": 1730370510000,
								"key_as_string": "2024-10-31T10:28:30.000"
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
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2
					}
				},
				"timed_out": false,
				"took": 2
			},
			"start_time_in_millis": 1730384367347
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374060000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374110000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0",
			  count(*) AS "aggr__timeseries__count"
			FROM __quesma_table_name
			WHERE ("@timestamp">=fromUnixTimestamp64Milli(1730370296174) AND "@timestamp"<=
			  fromUnixTimestamp64Milli(1730370596174))
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0"
			ORDER BY "aggr__timeseries__key_0" ASC`,
	},
	{ // [1]
		TestName: "extended_bounds post keys (timezone calculations most tricky to get right)",
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
							"max": 1730370520000
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
			"query": {
				"bool": {
					"filter": [],
					"must": [
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-10-31T10:24:56.174Z",
									"lte": "2024-10-31T10:29:56.174Z"
								}
							}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1730383525854,
			"expiration_time_in_millis": 1730815525840,
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
					"timeseries": {
						"buckets": [
							{
								"c4a48962-08e6-4791-ae9e-b50f1b111488": {
									"value": 1.0
								},
								"doc_count": 1,
								"key": 1730370460000,
								"key_as_string": "2024-10-31T10:27:40.000"
							},
							{
								"doc_count": 0,
								"key": 1730370470000,
								"key_as_string": "2024-10-31T10:27:50.000"
							},
							{
								"doc_count": 0,
								"key": 1730370480000,
								"key_as_string": "2024-10-31T10:28:00.000"
							},
							{
								"doc_count": 0,
								"key": 1730370490000,
								"key_as_string": "2024-10-31T10:28:10.000"
							},
							{
								"doc_count": 0,
								"key": 1730370500000,
								"key_as_string": "2024-10-31T10:28:20.000"
							},
							{
								"c4a48962-08e6-4791-ae9e-b50f1b111488": {
									"value": 1.0
								},
								"doc_count": 1,
								"key": 1730370510000,
								"key_as_string": "2024-10-31T10:28:30.000"
							},
							{
								"doc_count": 0,
								"key": 1730370520000,
								"key_as_string": "2024-10-31T10:28:40.000"
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
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2
					}
				},
				"timed_out": false,
				"took": 14
			},
			"start_time_in_millis": 1730383525840
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374060000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374110000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0",
			  count(*) AS "aggr__timeseries__count"
			FROM __quesma_table_name
			WHERE ("@timestamp">=fromUnixTimestamp64Milli(1730370296174) AND "@timestamp"<=
			  fromUnixTimestamp64Milli(1730370596174))
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0"
			ORDER BY "aggr__timeseries__key_0" ASC`,
	},
}
