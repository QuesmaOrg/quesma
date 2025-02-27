// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "github.com/QuesmaOrg/quesma/platform/model"

var AggregationTestsWithSpecialCharactersInFieldNames = []AggregationTestCase{
	{
		TestName: "Top metrics",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"top_metrics": {
								"metrics": {
									"field": "-@bytes"
								},
								"size": 1,
								"sort": {
									"-@timestamp": "desc"
								}
							}
						}
					},
					"date_histogram": {
						"field": "-@timestamp",
						"fixed_interval": "12h",
						"min_doc_count": 1
					}
				}
			},
			"fields": [
				{
					"field": "-@timestamp",
					"format": "date_time"
				},
				{
					"field": "timestamp",
					"format": "date_time"
				},
				{
					"field": "utc_time",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"exists": {
								"field": "message$*%:;"
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
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
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1711457737150,
			"expiration_time_in_millis": 1711889737150,
			"completion_time_in_millis": 1711457737169,
			"response": {
				"took": 19,
				"timed_out": false,
				"_shards": {
					"total": 1,
					"successful": 1,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 2190,
						"relation": "eq"
					},
					"max_score": null,
					"hits": []
				},
				"aggregations": {
					"0": {
						"buckets": [
							{
								"1": {
									"top": [
										{
											"sort": [
												"2024-03-17T10:59:44.162Z"
											],
											"metrics": {
												"bytes": 8279
											}
										}
									]
								},
								"key_as_string": "2024-03-17T00:00:00.000+01:00",
								"key": 1710630000000,
								"doc_count": 109
							},
							{
								"1": {
									"top": [
										{
											"sort": [
												"2024-03-17T21:48:16.637Z"
											],
											"metrics": {
												"bytes": 8909
											}
										}
									]
								},
								"key_as_string": "2024-03-17T12:00:00.000+01:00",
								"key": 1710673200000,
								"doc_count": 140
							}
						]
					}
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{}, // checking only the SQLs is enough for now
		ExpectedPancakeSQL: `WITH quesma_top_hits_group_table AS (
			  SELECT sum(count(*)) OVER () AS "metric____quesma_total_count_col_0",
				toInt64(toUnixTimestamp64Milli("__timestamp") / 43200000) AS
				"aggr__0__key_0", count(*) AS "aggr__0__count"
			  FROM __quesma_table_name
			  WHERE "message_____" IS NOT NULL
			  GROUP BY toInt64(toUnixTimestamp64Milli("__timestamp") / 43200000) AS
				"aggr__0__key_0"
			  ORDER BY "aggr__0__key_0" ASC) ,
			quesma_top_hits_join AS (
			  SELECT "group_table"."metric____quesma_total_count_col_0" AS
				"metric____quesma_total_count_col_0",
				"group_table"."aggr__0__key_0" AS "aggr__0__key_0",
				"group_table"."aggr__0__count" AS "aggr__0__count",
				"hit_table"."__bytes" AS "top_metrics__0__1_col_0",
				"hit_table"."__timestamp" AS "top_metrics__0__1_col_1",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__0__key_0" ORDER BY
				"__timestamp" DESC) AS "top_hits_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON ("group_table"."aggr__0__key_0"=
				toInt64(toUnixTimestamp64Milli("__timestamp") / 43200000))
			  WHERE "message_____" IS NOT NULL)
			SELECT "metric____quesma_total_count_col_0", "aggr__0__key_0", "aggr__0__count",
			  "top_metrics__0__1_col_0", "top_metrics__0__1_col_1", "top_hits_rank"
			FROM "quesma_top_hits_join"
			WHERE "top_hits_rank"<=1
			ORDER BY "aggr__0__key_0" ASC, "top_hits_rank" ASC`,
	},
}
