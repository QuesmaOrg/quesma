// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/testdata"
)

var TuringTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "empty results",
		QueryRequestJson: `
		{
			"aggs": {
				"2": {
					"aggs": {
						"3": {
							"terms": {
								"field": "score",
								"order": {
									"_count": "desc"
								},
								"shard_size": 25,
								"size": 5
							}
						}
					},
					"date_histogram": {
						"field": "@timestamp",
						"fixed_interval": "12h",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"size": 0
		}`,
		ExpectedResponse: `
		{
			"aggregations": {
				"2": {
					"buckets": null
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3__parent_count",
			  "aggr__2__3__key_0", "aggr__2__3__count"
			FROM (
			  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3__parent_count",
				"aggr__2__3__key_0", "aggr__2__3__count",
				dense_rank() OVER (ORDER BY "aggr__2__key_0" ASC) AS "aggr__2__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__3__count" DESC, "aggr__2__3__key_0" ASC) AS
				"aggr__2__3__order_1_rank"
			  FROM (
				SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(
				  toTimezone("@timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__3__parent_count", "score" AS "aggr__2__3__key_0",
				  count(*) AS "aggr__2__3__count"
				FROM __quesma_table_name
				GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(
				  toTimezone("@timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__key_0", "level" AS "aggr__2__3__key_0"))
			WHERE "aggr__2__3__order_1_rank"<=6
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__3__order_1_rank" ASC`,
	},
}
