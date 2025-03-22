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
		ExpectedPancakeSQL: `
			SELECT FLOOR(((__quesma_geo_lon("OriginLocation")+180)/360)*POWER(2, 8)) AS
			  "aggr__large-grid__key_0",
			  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat("OriginLocation")))+(1/COS(RADIANS(
			  __quesma_geo_lat("OriginLocation")))))/PI())/2*POWER(2, 8)) AS
			  "aggr__large-grid__key_1", count(*) AS "aggr__large-grid__count"
			FROM __quesma_table_name
			GROUP BY FLOOR(((__quesma_geo_lon("OriginLocation")+180)/360)*POWER(2, 8)) AS
			  "aggr__large-grid__key_0",
			  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat("OriginLocation")))+(1/COS(RADIANS(
			  __quesma_geo_lat("OriginLocation")))))/PI())/2*POWER(2, 8)) AS
			  "aggr__large-grid__key_1"
			ORDER BY "aggr__large-grid__count" DESC, "aggr__large-grid__key_0" ASC,
			  "aggr__large-grid__key_1" ASC
			LIMIT 10000`,
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
		ExpectedPancakeSQL: `
			SELECT FLOOR(((__quesma_geo_lon("OriginLocation")+180)/360)*POWER(2, 8))
			  AS "aggr__large-grid__key_0",
			  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat("OriginLocation")))+(1/COS(RADIANS(
			  __quesma_geo_lat("OriginLocation")))))/PI())/2*POWER(2, 8))
			  AS "aggr__large-grid__key_1", count(*) AS "aggr__large-grid__count"
			FROM __quesma_table_name
			GROUP BY FLOOR(((__quesma_geo_lon("OriginLocation")+180)/360)*POWER(2, 8))
			  AS "aggr__large-grid__key_0",
			  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat("OriginLocation")))+(1/COS(RADIANS(
			  __quesma_geo_lat("OriginLocation")))))/PI())/2*POWER(2, 8)) AS "aggr__large-grid__key_1"
			ORDER BY "aggr__large-grid__count" DESC, "aggr__large-grid__key_0" ASC,
			  "aggr__large-grid__key_1" ASC
			LIMIT 3`,
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
		ExpectedPancakeSQL: `
			SELECT "aggr__terms__parent_count", "aggr__terms__key_0", "aggr__terms__count",
			  "aggr__terms__large-grid__key_0", "aggr__terms__large-grid__key_1",
			  "aggr__terms__large-grid__count", "metric__terms__large-grid__avg_col_0"
			FROM (
			  SELECT "aggr__terms__parent_count", "aggr__terms__key_0",
				"aggr__terms__count", "aggr__terms__large-grid__key_0",
				"aggr__terms__large-grid__key_1", "aggr__terms__large-grid__count",
				"metric__terms__large-grid__avg_col_0",
				dense_rank() OVER (ORDER BY "aggr__terms__count" DESC, "aggr__terms__key_0"
				ASC) AS "aggr__terms__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__terms__key_0" ORDER BY
				"aggr__terms__large-grid__count" DESC, "aggr__terms__large-grid__key_0" ASC,
				"aggr__terms__large-grid__key_1" ASC) AS
				"aggr__terms__large-grid__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__terms__parent_count",
				  COALESCE("AvgTicketPrice", 'N/A') AS "aggr__terms__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__terms__key_0") AS
				  "aggr__terms__count",
				  FLOOR(((__quesma_geo_lon("OriginLocation")+180)/360)*POWER(2, 8)) AS
				  "aggr__terms__large-grid__key_0",
				  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat("OriginLocation")))+(1/COS(
				  RADIANS(__quesma_geo_lat("OriginLocation")))))/PI())/2*POWER(2, 8)) AS
				  "aggr__terms__large-grid__key_1",
				  count(*) AS "aggr__terms__large-grid__count",
				  avgOrNull("DistanceKilometers") AS "metric__terms__large-grid__avg_col_0"
				FROM __quesma_table_name
				GROUP BY COALESCE("AvgTicketPrice", 'N/A') AS "aggr__terms__key_0",
				  FLOOR(((__quesma_geo_lon("OriginLocation")+180)/360)*POWER(2, 8)) AS
				  "aggr__terms__large-grid__key_0",
				  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat("OriginLocation")))+(1/COS(
				  RADIANS(__quesma_geo_lat("OriginLocation")))))/PI())/2*POWER(2, 8)) AS
				  "aggr__terms__large-grid__key_1"))
			WHERE ("aggr__terms__order_1_rank"<=2 AND
			  "aggr__terms__large-grid__order_1_rank"<=3)
			ORDER BY "aggr__terms__order_1_rank" ASC,
			  "aggr__terms__large-grid__order_1_rank" ASC`,
	},
}
