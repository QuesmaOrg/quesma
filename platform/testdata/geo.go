// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "github.com/QuesmaOrg/quesma/platform/model"

var AggregationTestsWithGeographicalCoordinates = []AggregationTestCase{
	{ // [0]
		TestName: "simplest geotile_grid",
		QueryRequestJson: `
		{
			"aggs": {
				"large-grid": {
					"geotile_grid": {
						"field": "OriginLocation",
						"precision": 8
					}
				}
			},
			"size": 0
		}`,
		ExpectedResponse: `
		{
			"took": 70,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 10000,
					"relation": "gte"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"large-grid": {
					"buckets": [
						{
							"key": "8/136/95",
							"doc_count": 416
						},
						{
							"key": "8/134/91",
							"doc_count": 360
						},
						{
							"key": "8/72/128",
							"doc_count": 283
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__large-grid__key_0", 136.),
				model.NewQueryResultCol("aggr__large-grid__key_1", 95.),
				model.NewQueryResultCol("aggr__large-grid__count", 416),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__large-grid__key_0", 134.),
				model.NewQueryResultCol("aggr__large-grid__key_1", 91.),
				model.NewQueryResultCol("aggr__large-grid__count", 360),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__large-grid__key_0", 72.),
				model.NewQueryResultCol("aggr__large-grid__key_1", 128.),
				model.NewQueryResultCol("aggr__large-grid__count", 283),
			}},
		},
		ExpectedPancakeSQL: "SELECT FLOOR(((__quesma_geo_lon(`OriginLocation`)+180)/360)*POWER(2, 8)) AS\n" +
			"  `aggr__large-grid__key_0`,\n" +
			"  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat(`OriginLocation`)))+(1/COS(RADIANS(\n" +
			"  __quesma_geo_lat(`OriginLocation`)))))/PI())/2*POWER(2, 8)) AS\n" +
			"  `aggr__large-grid__key_1`, count(*) AS `aggr__large-grid__count`\n" +
			"FROM `__quesma_table_name`\n" +
			"GROUP BY FLOOR(((__quesma_geo_lon(`OriginLocation`)+180)/360)*POWER(2, 8)) AS\n" +
			"  `aggr__large-grid__key_0`,\n" +
			"  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat(`OriginLocation`)))+(1/COS(RADIANS(\n" +
			"  __quesma_geo_lat(`OriginLocation`)))))/PI())/2*POWER(2, 8)) AS\n" +
			"  `aggr__large-grid__key_1`\n" +
			"ORDER BY `aggr__large-grid__count` DESC, `aggr__large-grid__key_0` ASC,\n" +
			"  `aggr__large-grid__key_1` ASC\n" +
			"LIMIT 10000",
	},
	{ // [1]
		TestName: "geotile_grid with size",
		QueryRequestJson: `
		{
			"aggs": {
				"large-grid": {
					"geotile_grid": {
						"field": "OriginLocation",
						"precision": 8,
						"size": 3
					}
				}
			},
			"size": 0
		}`,
		ExpectedResponse: `
		{
			"took": 70,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 10000,
					"relation": "gte"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"large-grid": {
					"buckets": [
						{
							"key": "8/136/95",
							"doc_count": 416
						},
						{
							"key": "8/134/91",
							"doc_count": 360
						},
						{
							"key": "8/72/128",
							"doc_count": 283
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__large-grid__key_0", 136.),
				model.NewQueryResultCol("aggr__large-grid__key_1", 95.),
				model.NewQueryResultCol("aggr__large-grid__count", 416),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__large-grid__key_0", 134.),
				model.NewQueryResultCol("aggr__large-grid__key_1", 91.),
				model.NewQueryResultCol("aggr__large-grid__count", 360),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__large-grid__key_0", 72.),
				model.NewQueryResultCol("aggr__large-grid__key_1", 128.),
				model.NewQueryResultCol("aggr__large-grid__count", 283),
			}},
		},
		ExpectedPancakeSQL: "SELECT FLOOR(((__quesma_geo_lon(`OriginLocation`)+180)/360)*POWER(2, 8))\n" +
			"  AS `aggr__large-grid__key_0`,\n" +
			"  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat(`OriginLocation`)))+(1/COS(RADIANS(\n" +
			"  __quesma_geo_lat(`OriginLocation`)))))/PI())/2*POWER(2, 8))\n" +
			"  AS `aggr__large-grid__key_1`, count(*) AS `aggr__large-grid__count`\n" +
			"FROM `__quesma_table_name`\n" +
			"GROUP BY FLOOR(((__quesma_geo_lon(`OriginLocation`)+180)/360)*POWER(2, 8))\n" +
			"  AS `aggr__large-grid__key_0`,\n" +
			"  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat(`OriginLocation`)))+(1/COS(RADIANS(\n" +
			"  __quesma_geo_lat(`OriginLocation`)))))/PI())/2*POWER(2, 8)) AS `aggr__large-grid__key_1`\n" +
			"ORDER BY `aggr__large-grid__count` DESC, `aggr__large-grid__key_0` ASC,\n" +
			"  `aggr__large-grid__key_1` ASC\n" +
			"LIMIT 3",
	},
	{ // [2]
		TestName: "geotile_grid with some other aggregations",
		QueryRequestJson: `
		{
			"aggs": {
				"terms": {
					"terms": {
						"field": "AvgTicketPrice",
						"size": 2,
						"missing": "N/A"
					},
					"aggs": {
						"large-grid": {
							"geotile_grid": {
								"field": "OriginLocation",
								"precision": 8,
								"size": 3
							},
							"aggs": {
								"avg": {
									"avg": {
										"field": "DistanceKilometers"
									}
								}
							}
						}
					}
				}
			},
			"size": 0
		}`,
		ExpectedResponse: `
		{
			"took": 70,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 10000,
					"relation": "gte"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"terms": {
					"sum_other_doc_count": 8580,
					"buckets": [
						{
							"key": "N/A",
							"doc_count": 1000,
							"large-grid": {
								"buckets": [	
									{
										"key": "8/136/95",	
										"doc_count": 416,
										"avg": {
											"value": 123.45	
										}
									},
									{
										"key": "8/134/91",	
										"doc_count": 360,
										"avg": {
											"value": 100.2	
										}
									}
								]
							}
						},
						{
							"key": 420,
							"doc_count": 420,
							"large-grid": {
								"buckets": [	
									{
										"key": "8/72/128",	
										"doc_count": 283,
										"avg": {
											"value": 50.5	
										}
									}
								]
							}
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__terms__parent_count", int64(10000)),
				model.NewQueryResultCol("aggr__terms__key_0", "N/A"),
				model.NewQueryResultCol("aggr__terms__count", int64(1000)),
				model.NewQueryResultCol("aggr__terms__large-grid__key_0", 136.),
				model.NewQueryResultCol("aggr__terms__large-grid__key_1", 95.),
				model.NewQueryResultCol("aggr__terms__large-grid__count", int64(416)),
				model.NewQueryResultCol("metric__terms__large-grid__avg_col_0", 123.45),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__terms__parent_count", int64(10000)),
				model.NewQueryResultCol("aggr__terms__key_0", "N/A"),
				model.NewQueryResultCol("aggr__terms__count", int64(1000)),
				model.NewQueryResultCol("aggr__terms__large-grid__key_0", 134.),
				model.NewQueryResultCol("aggr__terms__large-grid__key_1", 91.),
				model.NewQueryResultCol("aggr__terms__large-grid__count", int64(360)),
				model.NewQueryResultCol("metric__terms__large-grid__avg_col_0", 100.2),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__terms__parent_count", int64(10000)),
				model.NewQueryResultCol("aggr__terms__key_0", 420),
				model.NewQueryResultCol("aggr__terms__count", int64(420)),
				model.NewQueryResultCol("aggr__terms__large-grid__key_0", 72.),
				model.NewQueryResultCol("aggr__terms__large-grid__key_1", 128.),
				model.NewQueryResultCol("aggr__terms__large-grid__count", int64(283)),
				model.NewQueryResultCol("metric__terms__large-grid__avg_col_0", 50.5),
			}},
		},
		ExpectedPancakeSQL: "SELECT `aggr__terms__parent_count`, `aggr__terms__key_0`, `aggr__terms__count`,\n" +
			"  `aggr__terms__large-grid__key_0`, `aggr__terms__large-grid__key_1`,\n" +
			"  `aggr__terms__large-grid__count`, `metric__terms__large-grid__avg_col_0`\n" +
			"FROM (\n" +
			"  SELECT `aggr__terms__parent_count`, `aggr__terms__key_0`,\n" +
			"    `aggr__terms__count`, `aggr__terms__large-grid__key_0`,\n" +
			"    `aggr__terms__large-grid__key_1`, `aggr__terms__large-grid__count`,\n" +
			"    `metric__terms__large-grid__avg_col_0`,\n" +
			"    dense_rank() OVER (ORDER BY `aggr__terms__count` DESC, `aggr__terms__key_0`\n" +
			"    ASC) AS `aggr__terms__order_1_rank`,\n" +
			"    dense_rank() OVER (PARTITION BY `aggr__terms__key_0` ORDER BY\n" +
			"    `aggr__terms__large-grid__count` DESC, `aggr__terms__large-grid__key_0` ASC,\n" +
			"    `aggr__terms__large-grid__key_1` ASC) AS\n" +
			"    `aggr__terms__large-grid__order_1_rank`\n" +
			"  FROM (\n" +
			"    SELECT sum(count(*)) OVER () AS `aggr__terms__parent_count`,\n" +
			"      COALESCE(`AvgTicketPrice`, 'N/A') AS `aggr__terms__key_0`,\n" +
			"      sum(count(*)) OVER (PARTITION BY `aggr__terms__key_0`) AS\n" +
			"      `aggr__terms__count`,\n" +
			"      FLOOR(((__quesma_geo_lon(`OriginLocation`)+180)/360)*POWER(2, 8)) AS\n" +
			"      `aggr__terms__large-grid__key_0`,\n" +
			"      FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat(`OriginLocation`)))+(1/COS(\n" +
			"      RADIANS(__quesma_geo_lat(`OriginLocation`)))))/PI())/2*POWER(2, 8)) AS\n" +
			"      `aggr__terms__large-grid__key_1`,\n" +
			"      count(*) AS `aggr__terms__large-grid__count`,\n" +
			"      avgOrNull(`DistanceKilometers`) AS `metric__terms__large-grid__avg_col_0`\n" +
			"    FROM `__quesma_table_name`\n" +
			"    GROUP BY COALESCE(`AvgTicketPrice`, 'N/A') AS `aggr__terms__key_0`,\n" +
			"      FLOOR(((__quesma_geo_lon(`OriginLocation`)+180)/360)*POWER(2, 8)) AS\n" +
			"      `aggr__terms__large-grid__key_0`,\n" +
			"      FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat(`OriginLocation`)))+(1/COS(\n" +
			"      RADIANS(__quesma_geo_lat(`OriginLocation`)))))/PI())/2*POWER(2, 8)) AS\n" +
			"      `aggr__terms__large-grid__key_1`))\n" +
			"WHERE (`aggr__terms__order_1_rank`<=2 AND\n" +
			"  `aggr__terms__large-grid__order_1_rank`<=3)\n" +
			"ORDER BY `aggr__terms__order_1_rank` ASC,\n" +
			"  `aggr__terms__large-grid__order_1_rank` ASC",
	},
}
