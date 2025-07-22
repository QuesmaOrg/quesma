// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/testdata"
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
					"buckets": []
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{},
		ExpectedPancakeSQL: "SELECT `aggr__2__key_0`, `aggr__2__count`, `aggr__2__3__parent_count`,\n" +
			"  `aggr__2__3__key_0`, `aggr__2__3__count`\n" +
			"FROM (\n" +
			"  SELECT `aggr__2__key_0`, `aggr__2__count`, `aggr__2__3__parent_count`,\n" +
			"    `aggr__2__3__key_0`, `aggr__2__3__count`,\n" +
			"    dense_rank() OVER (ORDER BY `aggr__2__key_0` ASC) AS `aggr__2__order_1_rank`,\n" +
			"    dense_rank() OVER (PARTITION BY `aggr__2__key_0` ORDER BY\n" +
			"    `aggr__2__3__count` DESC, `aggr__2__3__key_0` ASC) AS\n" +
			"    `aggr__2__3__order_1_rank`\n" +
			"  FROM (\n" +
			"    SELECT toInt64((toUnixTimestamp64Milli(`@timestamp`)+timeZoneOffset(\n" +
			"      toTimezone(`@timestamp`, 'Europe/Warsaw'))*1000) / 43200000) AS\n" +
			"      `aggr__2__key_0`,\n" +
			"      sum(count(*)) OVER (PARTITION BY `aggr__2__key_0`) AS `aggr__2__count`,\n" +
			"      sum(count(*)) OVER (PARTITION BY `aggr__2__key_0`) AS\n" +
			"      `aggr__2__3__parent_count`, `score` AS `aggr__2__3__key_0`,\n" +
			"      count(*) AS `aggr__2__3__count`\n" +
			"    FROM `__quesma_table_name`\n" +
			"    GROUP BY toInt64((toUnixTimestamp64Milli(`@timestamp`)+timeZoneOffset(\n" +
			"      toTimezone(`@timestamp`, 'Europe/Warsaw'))*1000) / 43200000) AS\n" +
			"      `aggr__2__key_0`, `score` AS `aggr__2__3__key_0`))\n" +
			"WHERE `aggr__2__3__order_1_rank`<=6\n" +
			"ORDER BY `aggr__2__order_1_rank` ASC, `aggr__2__3__order_1_rank` ASC",
	},
}
