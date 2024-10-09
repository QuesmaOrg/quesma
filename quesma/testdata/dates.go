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
	{ // [0]
		TestName: "simple max/min aggregation as 2 siblings",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"other-filter": {
					"aggs": {
						"3": {
							"terms": {
								"field": "field",
								"order": {
									"_count": "desc"
								},
								"size": 15
							}
						}
					},
					"filters": {
						"filters": {
							"": {
								"bool": {
									"filter": [],
									"must": [
										{
											"match_phrase": {
												"a": "b"
											}
										},
										{
											"match_phrase": {
												"c": "d"
											}
										}
									],
									"must_not": [],
									"should": []
								}
							}
						}
					}
				}
			},
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
				model.NewQueryResultCol("aggr__other-filter__count", int64(4675)),
				model.NewQueryResultCol("aggr__other-filter__3__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__other-filter__3__key_0", "field"),
				model.NewQueryResultCol("aggr__other-filter__3__count", int64(442)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%'))) OVER () AS
			  "aggr__other-filter__count",
			  sum(countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%'))) OVER () AS
			  "aggr__other-filter__3__parent_count",
			  "field" AS "aggr__other-filter__3__key_0",
			  countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%')) AS
			  "aggr__other-filter__3__count"
			FROM __quesma_table_name
			GROUP BY "field" AS "aggr__other-filter__3__key_0"
			ORDER BY "aggr__other-filter__3__count" DESC,
			  "aggr__other-filter__3__key_0" ASC
			LIMIT 16`,
	},
	{ // [0]
		TestName: "simple max/min aggregation as 2 siblings",
		QueryRequestJson: `
		{
			"aggs": {
				"q": {
					"aggs": {
						"time_buckets": {
							"aggs": {
								"count": {
									"bucket_script": {
										"buckets_path": "_count",
										"script": {
											"lang": "expression",
											"source": "_value"
										}
									}
								}
							},
							"date_histogram": {
								"extended_bounds": {
									"max": 1728507732621,
									"min": 1728507729621
								},
								"field": "@timestamp",
								"fixed_interval": "100ms",
								"min_doc_count": 0,
								"time_zone": "Europe/Warsaw"
							},
							"meta": {
								"type": "time_buckets"
							}
						}
					},
					"filters": {
						"filters": {
							"*": {
								"query_string": {
									"query": "*"
								}
							}
						}
					},
					"meta": {
						"type": "split"
					}
				}
			},
			"query": {
				"bool": {
					"filter": {
						"bool": {
							"filter": [],
							"must": [],
							"must_not": [],
							"should": []
						}
					},
					"must": [
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-10-09T21:02:09.621Z",
									"lte": "2024-10-09T21:02:12.621Z"
								}
							}
						}
					]
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
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
				model.NewQueryResultCol("aggr__other-filter__count", int64(4675)),
				model.NewQueryResultCol("aggr__other-filter__3__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__other-filter__3__key_0", "field"),
				model.NewQueryResultCol("aggr__other-filter__3__count", int64(442)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%'))) OVER () AS
			  "aggr__other-filter__count",
			  sum(countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%'))) OVER () AS
			  "aggr__other-filter__3__parent_count",
			  "field" AS "aggr__other-filter__3__key_0",
			  countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%')) AS
			  "aggr__other-filter__3__count"
			FROM __quesma_table_name
			GROUP BY "field" AS "aggr__other-filter__3__key_0"
			ORDER BY "aggr__other-filter__3__count" DESC,
			  "aggr__other-filter__3__key_0" ASC
			LIMIT 16`,
	},
}
