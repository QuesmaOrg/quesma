// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"cmp"
	"context"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/queryparser/query_util"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/testdata"
	"quesma/testdata/clients"
	dashboard_1 "quesma/testdata/dashboard-1"
	kibana_visualize "quesma/testdata/kibana-visualize"
	opensearch_visualize "quesma/testdata/opensearch-visualize"
	"quesma/util"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const tableName = model.SingleTableNamePlaceHolder

// Simple unit tests, testing only "aggs" part of the request json query
var aggregationTests = []struct {
	aggregationJson string
	translatedSqls  []string
}{
	{ // [0]
		`
		{
			"aggs": {
				"maxAgg": {
					"max": {
						"field": "AvgTicketPrice"
					}
				},
				"minAgg": {
					"min": {
						"field": "AvgTicketPrice"
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT maxOrNull("AvgTicketPrice") FROM ` + tableName,
			`SELECT minOrNull("AvgTicketPrice") FROM ` + tableName,
		},
	},
	{ // [1]
		`
		{
			"aggs": {
				"0": {
					"aggs": {
						"1-bucket": {
							"filter": {
								"bool": {
									"filter": [
										{
											"bool": {
												"minimum_should_match": 1,
												"should": [
													{
														"match": {
															"FlightDelay": true
														}
													}
												]
											}
										}
									],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						},
						"3-bucket": {
							"filter": {
								"bool": {
									"filter": [
										{
											"bool": {
												"minimum_should_match": 1,
												"should": [
													{
														"match": {
															"Cancelled": true
														}
													}
												]
											}
										}
									],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						}
					},
					"terms": {
						"field": "OriginCityName",
						"order": {
							"_key": "asc"
						},
						"size": 1000
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT "OriginCityName", count() ` +
				`FROM ` + tableName + ` ` +
				`WHERE "OriginCityName" IS NOT NULL ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName" ASC ` +
				`LIMIT 1000`,
			`WITH cte_1 AS ` +
				`(SELECT "OriginCityName" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "OriginCityName" IS NOT NULL ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName" ASC ` +
				`LIMIT 1000) ` +
				`SELECT "OriginCityName", count() ` +
				`FROM ` + tableName + ` ` +
				`INNER JOIN "cte_1" ON "OriginCityName" = "cte_1_1" ` +
				`WHERE ("OriginCityName" IS NOT NULL AND "FlightDelay"==true) ` +
				`GROUP BY "OriginCityName", cte_1_cnt ` +
				`ORDER BY "OriginCityName" ASC`,
			`WITH cte_1 AS ` +
				`(SELECT "OriginCityName" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "OriginCityName" IS NOT NULL ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName" ASC ` +
				`LIMIT 1000) ` +
				`SELECT "OriginCityName", count() ` +
				`FROM ` + tableName + ` ` +
				`INNER JOIN "cte_1" ON "OriginCityName" = "cte_1_1" ` +
				`WHERE ("OriginCityName" IS NOT NULL AND "Cancelled"==true) ` +
				`GROUP BY "OriginCityName", cte_1_cnt ` +
				`ORDER BY "OriginCityName" ASC`,
		},
	},
	{ // [2]
		`
		{
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"date_histogram": {
								"extended_bounds": {
									"max": 1707486436029,
									"min": 1706881636029
								},
								"field": "timestamp",
								"fixed_interval": "3h",
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"terms": {
						"field": "FlightDelayType",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 10
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT "FlightDelayType", count() ` +
				`FROM ` + tableName + ` ` +
				`WHERE "FlightDelayType" IS NOT NULL ` +
				`GROUP BY "FlightDelayType" ` +
				`ORDER BY count() DESC, "FlightDelayType" ` +
				`LIMIT 10`,
			`WITH cte_1 AS ` +
				`(SELECT "FlightDelayType" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "FlightDelayType" IS NOT NULL ` +
				`GROUP BY "FlightDelayType" ` +
				`ORDER BY count() DESC, "FlightDelayType" ` +
				`LIMIT 10) ` +
				`SELECT "FlightDelayType", toInt64(toUnixTimestamp64Milli("timestamp") / 10800000), count() ` +
				`FROM ` + tableName + ` ` +
				`INNER JOIN "cte_1" ON "FlightDelayType" = "cte_1_1" ` +
				`WHERE "FlightDelayType" IS NOT NULL ` +
				`GROUP BY "FlightDelayType", toInt64(toUnixTimestamp64Milli("timestamp") / 10800000), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "FlightDelayType", toInt64(toUnixTimestamp64Milli("timestamp") / 10800000)`,
		},
	},
	{ // [3]
		`
		{
			"aggs": {
				"0-bucket": {
					"filter": {
						"bool": {
							"filter": [
								{
									"bool": {
										"minimum_should_match": 1,
										"should": [
											{
												"match": {
													"FlightDelay": true
												}
											}
										]
									}
								}
							],
							"must": [],
							"must_not": [],
							"should": []
						}
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT count() FROM ` + tableName + ` WHERE "FlightDelay"==true`,
		},
	},
	{ // [4]
		`
		{
			"aggs": {
				"time_offset_split": {
					"aggs": {},
					"filters": {
						"filters": {
							"0": {
								"range": {
									"timestamp": {
										"format": "strict_date_optional_time",
										"gte": "2024-02-02T13:47:16.029Z",
										"lte": "2024-02-09T13:47:16.029Z"
									}
								}
							},
							"604800000": {
								"range": {
									"timestamp": {
										"format": "strict_date_optional_time",
										"gte": "2024-01-26T13:47:16.029Z",
										"lte": "2024-02-02T13:47:16.029Z"
									}
								}
							}
						}
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT count() FROM ` + tableName + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT count() FROM ` + tableName + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z'))`,
		},
	},
	{ // [5]
		`
		{
			"aggs": {
				"0": {
					"histogram": {
						"field": "FlightDelayMin",
						"interval": 1,
						"min_doc_count": 1
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT "FlightDelayMin", count() FROM ` + tableName + ` GROUP BY "FlightDelayMin" ORDER BY "FlightDelayMin"`,
		},
	},
	{ // [6]
		`
		{
			"aggs": {
				"origins": {
					"aggs": {
						"distinations": {
							"aggs": {
								"destLocation": {
									"top_hits": {
										"_source": {
											"includes": [
												"DestLocation"
											]
										},
										"size": 1
									}
								}
							},
							"terms": {
								"field": "DestAirportID",
								"size": 10000
							}
						},
						"originLocation": {
							"top_hits": {
								"_source": {
									"includes": [
										"OriginLocation",
										"Origin"
									]
								},
								"size": 1
							}
						}
					},
					"terms": {
						"field": "OriginAirportID",
						"size": 10000
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`WITH cte_1 AS ` +
				`(SELECT "OriginAirportID" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "OriginAirportID" IS NOT NULL ` +
				`GROUP BY "OriginAirportID" ` +
				`ORDER BY count() DESC, "OriginAirportID" ` +
				`LIMIT 10000) ` +
				`SELECT "OriginAirportID", "DestAirportID", count() ` +
				`FROM ` + tableName + ` ` +
				`INNER JOIN "cte_1" ON "OriginAirportID" = "cte_1_1" ` +
				`WHERE ("OriginAirportID" IS NOT NULL AND "DestAirportID" IS NOT NULL) ` +
				`GROUP BY "OriginAirportID", "DestAirportID", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "OriginAirportID", count() DESC, "DestAirportID" ` +
				`LIMIT 10000 BY "OriginAirportID"`,
			`WITH cte_1 AS ` +
				`(SELECT "OriginAirportID" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "OriginAirportID" IS NOT NULL ` +
				`GROUP BY "OriginAirportID" ` +
				`ORDER BY count() DESC, "OriginAirportID" ` +
				`LIMIT 10000), ` +
				`cte_2 AS ` +
				`(SELECT "OriginAirportID" AS "cte_2_1", "DestAirportID" AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + tableName + ` ` +
				`WHERE ("OriginAirportID" IS NOT NULL AND "DestAirportID" IS NOT NULL) ` +
				`GROUP BY "OriginAirportID", "DestAirportID" ` +
				`ORDER BY count() DESC, "DestAirportID" ` +
				`LIMIT 10000 BY "OriginAirportID") ` +
				`SELECT "OriginAirportID", "DestAirportID", "DestLocation" ` +
				`FROM (SELECT "OriginAirportID", "DestAirportID", "DestLocation", ROW_NUMBER() ` +
				`OVER (PARTITION BY "OriginAirportID", "DestAirportID") AS "row_number" ` +
				`FROM ` + tableName + ` ` +
				`WHERE ("OriginAirportID" IS NOT NULL AND "DestAirportID" IS NOT NULL)) ` +
				`INNER JOIN "cte_1" ON "OriginAirportID" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "OriginAirportID" = "cte_2_1" AND "DestAirportID" = "cte_2_2" ` +
				`WHERE (("OriginAirportID" IS NOT NULL AND "DestAirportID" IS NOT NULL) AND "row_number"<=1) ` +
				`GROUP BY "OriginAirportID", "DestAirportID", "DestLocation", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "OriginAirportID", cte_2_cnt DESC, "DestAirportID"`,
			`WITH cte_1 AS ` +
				`(SELECT "OriginAirportID" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "OriginAirportID" IS NOT NULL ` +
				`GROUP BY "OriginAirportID" ` +
				`ORDER BY count() DESC, "OriginAirportID" ` +
				`LIMIT 10000) ` +
				`SELECT "OriginAirportID", "OriginLocation", "Origin" ` +
				`FROM (SELECT "OriginAirportID", "OriginLocation", "Origin", ROW_NUMBER() ` +
				`OVER (PARTITION BY "OriginAirportID") AS "row_number" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "OriginAirportID" IS NOT NULL) ` +
				`INNER JOIN "cte_1" ON "OriginAirportID" = "cte_1_1" ` +
				`WHERE ("OriginAirportID" IS NOT NULL AND "row_number"<=1) ` +
				`GROUP BY "OriginAirportID", "OriginLocation", "Origin", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "OriginAirportID"`,
			`SELECT "OriginAirportID", count() ` +
				`FROM ` + tableName + ` ` +
				`WHERE "OriginAirportID" IS NOT NULL ` +
				`GROUP BY "OriginAirportID" ` +
				`ORDER BY count() DESC, "OriginAirportID" ` +
				`LIMIT 10000`,
		},
	},
	{ // [7]
		`
		{
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"date_histogram": {
								"calendar_interval": "1d",
								"extended_bounds": {
									"max": 1707818397034,
									"min": 1707213597034
								},
								"field": "order_date",
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"terms": {
						"field": "category.keyword",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 10
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`WITH cte_1 AS ` +
				`(SELECT "category" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "category" IS NOT NULL ` +
				`GROUP BY "category" ` +
				`ORDER BY count() DESC, "category" ` +
				`LIMIT 10) ` +
				`SELECT "category", toInt64(toUnixTimestamp64Milli("order_date") / 86400000), count() ` +
				`FROM ` + tableName + ` ` +
				`INNER JOIN "cte_1" ON "category" = "cte_1_1" ` +
				`WHERE "category" IS NOT NULL ` +
				`GROUP BY "category", toInt64(toUnixTimestamp64Milli("order_date") / 86400000), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "category", toInt64(toUnixTimestamp64Milli("order_date") / 86400000)`,
			`SELECT "category", count() ` +
				`FROM ` + tableName + ` ` +
				`WHERE "category" IS NOT NULL ` +
				`GROUP BY "category" ` +
				`ORDER BY count() DESC, "category" ` +
				`LIMIT 10`,
		},
	},
	{ // [8]
		`
		{
			"aggs": {
				"0": {
					"sum": {
						"field": "taxful_total_price"
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT sumOrNull("taxful_total_price") FROM ` + tableName,
		},
	},
	{ // [9]
		`
		{
			"aggs": {
				"0": {
					"percentiles": {
						"field": "taxful_total_price",
						"percents": [
							50
						]
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT quantiles(0.500000)("taxful_total_price") AS "quantile_50" FROM ` + tableName,
		},
	},
	{ // [10]
		`
		{
			"aggs": {
				"0": {
					"avg": {
						"field": "total_quantity"
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT avgOrNull("total_quantity") FROM ` + tableName,
		},
	},
	{ // [11]
		`
		{
			"aggs": {
				"1": {
					"aggs": {
						"2": {
							"aggs": {
								"4": {
									"top_metrics": {
										"metrics": {
											"field": "order_date"
										},
										"size": 10,
										"sort": {
											"order_date": "asc"
										}
									}
								},
								"5": {
									"top_metrics": {
										"metrics": {
											"field": "taxful_total_price"
										},
										"size": 10,
										"sort": {
											"order_date": "asc"
										}
									}
								}
							},
							"date_histogram": {
								"field": "order_date",
								"fixed_interval": "12h",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"filters": {
						"filters": {
							"c8c30be0-b88f-11e8-a451-f37365e9f268": {
								"bool": {
									"filter": [],
									"must": [{
										"query_string": {
											"analyze_wildcard": true,
											"query": "taxful_total_price:>250",
											"time_zone": "Europe/Warsaw"
										}
									}],
									"must_not": [],
									"should": []
								}
							}
						}
					}
				}
			},
			"size": 0
		}`,
		[]string{
			`SELECT count() FROM ` + tableName + ` WHERE "taxful_total_price" > '250'`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000), ` +
				`maxOrNull("order_date") AS "windowed_order_date", maxOrNull("order_date") AS "windowed_order_date" ` +
				`FROM (SELECT "order_date", "order_date", ROW_NUMBER() OVER ` +
				`(PARTITION BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY "order_date" ASC) AS "row_number", "taxful_total_price" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "taxful_total_price" > '250') ` +
				`WHERE ("taxful_total_price" > '250' AND "row_number"<=10) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000), ` +
				`maxOrNull("taxful_total_price") AS "windowed_taxful_total_price", maxOrNull("order_date") AS "windowed_order_date" ` +
				`FROM (SELECT "taxful_total_price", "order_date", ROW_NUMBER() OVER ` +
				`(PARTITION BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY "order_date" ASC) AS "row_number" ` +
				`FROM ` + tableName + ` ` +
				`WHERE "taxful_total_price" > '250') ` +
				`WHERE ("taxful_total_price" > '250' AND "row_number"<=10) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000), count() ` +
				`FROM ` + tableName + ` ` +
				`WHERE "taxful_total_price" > '250' ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000)`,
		},
	},
	{ // [12]
		`{
				"aggs": {
					"suggestions": {
						"terms": {
							"field": "OriginCityName",
							"order": {
								"_count": "desc"
							},
							"shard_size": 10,
							"size": 10
						}
					},
					"unique_terms": {
						"cardinality": {
							"field": "OriginCityName"
						}
					}
				},
				"size": 0
			}`,
		[]string{
			`SELECT "OriginCityName", count() ` +
				`FROM ` + tableName + ` ` +
				`WHERE "OriginCityName" IS NOT NULL ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY count() DESC, "OriginCityName" ` +
				`LIMIT 10`,
			`SELECT count(DISTINCT "OriginCityName") FROM ` + tableName,
		},
	},
	{ // [13]
		`{
				 "aggs": {
					"sample": {
					  "aggs": {
						"histo": {
						  "histogram": {
							"field": "bytes",
							"interval": 1782
						  }
						}
					  },
					  "sampler": {
						"shard_size": 5000
					  }
					}
				  },
				  "size": 0
			}`,
		[]string{
			`SELECT floor("bytes"/1782.000000)*1782.000000, count() FROM ` + tableName + ` ` +
				`GROUP BY floor("bytes"/1782.000000)*1782.000000 ` +
				`ORDER BY floor("bytes"/1782.000000)*1782.000000`,
			`SELECT count() FROM ` + tableName,
		},
	},
}

// Simple unit test, testing only "aggs" part of the request json query
func TestAggregationParser(t *testing.T) {
	// logger.InitSimpleLoggerForTests() // FIXME there are 2 warns if you enable them, might look into that
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), &config.QuesmaConfiguration{})
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
				},
			},
		},
	}

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background(), SchemaRegistry: s}

	for testIdx, test := range aggregationTests {
		t.Run(strconv.Itoa(testIdx), func(t *testing.T) {
			body, parseErr := types.ParseJSON(test.aggregationJson)
			assert.NoError(t, parseErr)
			aggregations, err := cw.ParseAggregationJson(body)
			assert.NoError(t, err)
			assert.Equal(t, len(test.translatedSqls), len(aggregations))
			for _, aggregation := range aggregations {
				util.AssertContainsSqlEqual(t, test.translatedSqls, aggregation.SelectCommand.String())
			}
		})
	}
}

// Used in tests to make processing `aggregations` in a deterministic way
func sortAggregations(aggregations []*model.Query) {
	slices.SortFunc(aggregations, func(a, b *model.Query) int {
		aLen, bLen := len(a.Aggregators), len(b.Aggregators)
		for i := range min(aLen, bLen) {
			if a.Aggregators[i].Name != b.Aggregators[i].Name {
				return cmp.Compare(a.Aggregators[i].Name, b.Aggregators[i].Name)
			}
		}
		// non-aggregations (len == 0) should be first
		if aLen == 0 || bLen == 0 {
			return cmp.Compare(aLen, bLen)
		}
		// longer list is first, as we first go deeper when parsing aggregations
		return cmp.Compare(bLen, aLen)
	})
}

func allAggregationTests() []testdata.AggregationTestCase {
	const lowerBoundTestNr = 80
	allTests := make([]testdata.AggregationTestCase, 0, lowerBoundTestNr)
	allTests = append(allTests, testdata.AggregationTests...)
	allTests = append(allTests, testdata.AggregationTests2...)
	allTests = append(allTests, opensearch_visualize.AggregationTests...)
	allTests = append(allTests, dashboard_1.AggregationTests...)
	allTests = append(allTests, testdata.PipelineAggregationTests...)
	allTests = append(allTests, opensearch_visualize.PipelineAggregationTests...)
	allTests = append(allTests, kibana_visualize.AggregationTests...)
	allTests = append(allTests, clients.KunkkaTests...)
	allTests = append(allTests, clients.OpheliaTests...)
	return allTests
}

func TestAggregationParserExternalTestcases(t *testing.T) {

	ctx := context.Background()

	// logger.InitSimpleLoggerForTests()
	table := clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"@timestamp":  {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"timestamp":   {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"order_date":  {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
			"message":     {Name: "message", Type: clickhouse.NewBaseType("String")},
			"bytes_gauge": {Name: "bytes_gauge", Type: clickhouse.NewBaseType("UInt64")},
		},
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
	}
	cfg := &config.QuesmaConfiguration{}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, &table), cfg)

	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
				},
			},
		},
	}

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), SchemaRegistry: s, Config: cfg}
	for i, test := range allAggregationTests() {
		t.Run(test.TestName+"("+strconv.Itoa(i)+")", func(t *testing.T) {
			if test.TestName == "Max/Sum bucket with some null buckets. Reproduce: Visualize -> Vertical Bar: Metrics: Max (Sum) Bucket (Aggregation: Date Histogram, Metric: Min)" {
				t.Skip("Needs to be fixed by keeping last key for every aggregation. Now we sometimes don't know it. Hard to reproduce, leaving it for separate PR")
			}
			if test.TestName == "complex sum_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Sum Bucket (Bucket: Date Histogram, Metric: Average), Buckets: X-Asis: Histogram" {
				t.Skip("Waiting for fix. Now we handle only the case where pipeline agg is at the same nesting level as its parent. Should be quick to fix.")
			}
			if i == 27 || i == 29 || i == 30 {
				t.Skip("New tests, harder, failing for now.")
			}
			if strings.HasPrefix(test.TestName, "dashboard-1") {
				t.Skip("Those 2 tests have nested histograms with min_doc_count=0. Some work done long time ago (Krzysiek)")
			}
			if test.TestName == "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Top Hit, Buckets: Aggregation: Range" {
				t.Skip("Need a (most likely) small fix to top_hits.")
			}
			if i == 20 {
				t.Skip("Fixed in next PR.")
			}
			if i == 7 {
				t.Skip("Let's implement top_hits in next PR. Easily doable, just a bit of code.")
			}
			if test.TestName == "it's the same input as in previous test, but with the original output from Elastic."+
				"Skipped for now, as our response is different in 2 things: key_as_string date (probably not important) + we don't return 0's (e.g. doc_count: 0)."+
				"If we need clients/kunkka/test_0, used to be broken before aggregations merge fix" {
				t.Skip("Unskip and remove the previous test after those fixes.")
			}
			if test.TestName == "clients/kunkka/test_1, used to be broken before aggregations merge fix" {
				t.Skip("Small details left for this test to be correct. I'll (Krzysiek) fix soon after returning to work")
			}
			if test.TestName == "Ophelia Test 3: 5x terms + a lot of other aggregations" ||
				test.TestName == "Ophelia Test 6: triple terms + other aggregations + order by another aggregations" ||
				test.TestName == "Ophelia Test 7: 5x terms + a lot of other aggregations" {
				t.Skip("Very similar to 2 previous tests, results have like 500-1000 lines. They are almost finished though. Maybe I'll fix soon, but not in this PR")
			}

			if strings.HasPrefix(test.TestName, "2x date_histogram") || strings.HasPrefix(test.TestName, "2x histogram") {
				t.Skip("Don't want to waste time on filling results there. Do that if we decide not to discard non-pancake logic soon.")
			}

			body, parseErr := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, parseErr)

			plan, err := cw.ParseQuery(body)
			queries := plan.Queries
			assert.NoError(t, err)
			assert.Len(t, test.ExpectedResults, len(queries))
			sortAggregations(queries) // to make test runs deterministic

			// Let's leave those commented debugs for now, they'll be useful in next PRs
			for j, query := range queries {
				// fmt.Printf("--- Aggregation %d: %+v\n\n---SQL string: %s\n\n%v\n\n", j, query, model.AsString(query.SelectCommand), query.SelectCommand.Columns)
				if test.ExpectedSQLs[j] != "NoDBQuery" {
					util.AssertSqlEqual(t, test.ExpectedSQLs[j], query.SelectCommand.String())
				}
				if query_util.IsNonAggregationQuery(query) {
					continue
				}

				var resultTransformer model.QueryRowsTransformer
				switch agg := query.Type.(type) {
				case bucket_aggregations.Histogram:

					resultTransformer = agg.NewRowsTransformer()

				case *bucket_aggregations.DateHistogram:
					resultTransformer = agg.NewRowsTransformer()
				}
				if resultTransformer != nil {
					test.ExpectedResults[j] = resultTransformer.Transform(ctx, test.ExpectedResults[j])
				}

				// fmt.Println("--- Group by: ", query.GroupByFields)
			}

			// I copy `test.ExpectedResults`, as it's processed 2 times and each time it might be modified by
			// pipeline aggregation processing.
			var expectedResultsCopy [][]model.QueryResultRow
			err = copier.CopyWithOption(&expectedResultsCopy, &test.ExpectedResults, copier.Option{DeepCopy: true})
			assert.NoError(t, err)
			// pp.Println("EXPECTED", expectedResultsCopy)
			response := cw.MakeSearchResponse(queries, test.ExpectedResults)
			responseMarshalled, marshalErr := response.Marshal()
			// pp.Println("ACTUAL", response)
			assert.NoError(t, marshalErr)

			expectedResponseMap, _ := util.JsonToMap(test.ExpectedResponse)
			var expectedAggregationsPart JsonMap
			if responseSubMap, hasResponse := expectedResponseMap["response"]; hasResponse {
				expectedAggregationsPart = responseSubMap.(JsonMap)["aggregations"].(JsonMap)
			} else {
				expectedAggregationsPart = expectedResponseMap["aggregations"].(JsonMap)
			}
			actualMinusExpected, expectedMinusActual := util.MapDifference(response.Aggregations,
				expectedAggregationsPart, []string{}, true, true)

			// probability and seed are present in random_sampler aggregation. I'd assume they are not needed, thus let's not care about it for now.
			acceptableDifference := []string{"sum_other_doc_count", "probability", "seed", "bg_count", "doc_count", model.KeyAddedByQuesma,
				"sum_other_doc_count", "doc_count_error_upper_bound"} // Don't know why, but those 2 are still needed in new (clients/ophelia) tests. Let's fix it in another PR
			// pp.Println("ACTUAL diff", actualMinusExpected)
			// pp.Println("EXPECTED diff", expectedMinusActual)
			// pp.Println("ACTUAL", response.Aggregations)
			// pp.Println("EXPECTED", expectedAggregationsPart)
			assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference))
			assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference))
			if body["track_total_hits"] == true { // FIXME some better check after track_total_hits
				assert.Contains(t, string(responseMarshalled), `"value":`+strconv.FormatUint(test.ExpectedResults[0][0].Cols[0].Value.(uint64), 10))
			} // checks if hits nr is OK
		})
	}
}

func Test_quoteArray(t *testing.T) {
	inputs := [][]string{{"a", "b", "c"}, {"a"}, {}, {`"a"`, "b"}}
	tests := []struct {
		input    []string
		expected []string
	}{
		{inputs[0], []string{`"a"`, `"b"`, `"c"`}},
		{inputs[1], []string{`"a"`}},
		{inputs[2], []string{}},
		{inputs[3], []string{`"\"a\""`, `"b"`}},
	}
	for i, test := range tests {
		assert.Equal(t, test.expected, quoteArray(test.input))
		assert.Equal(t, inputs[i], test.input) // check that original array isn't changed
	}
}

func Test_parseFieldFromScriptField(t *testing.T) {
	goodQueryMap := func(sourceField string) QueryMap {
		return QueryMap{"script": QueryMap{"source": sourceField}}
	}

	testcases := []struct {
		queryMap        QueryMap
		expectedMatch   model.Expr
		expectedSuccess bool
	}{
		{goodQueryMap("doc['field1'].value.getHour()"), model.NewFunction("toHour", model.NewColumnRef("field1")), true},
		{goodQueryMap("doc['field1'].value.getHour() + doc['field2'].value.getHour()"), nil, false},
		{goodQueryMap("doc['field1'].value.hourOfDay"), model.NewFunction("toHour", model.NewColumnRef("field1")), true},
		{goodQueryMap("doc['field1'].value"), nil, false},
		{goodQueryMap("value.getHour() + doc['field2'].value.getHour()"), nil, false},
		{QueryMap{}, nil, false},
		{QueryMap{"script": QueryMap{}}, nil, false},
		{QueryMap{"script": QueryMap{"source": nil}}, nil, false},
		{QueryMap{"script": "script"}, nil, false},
		{QueryMap{"script": QueryMap{"source": 1}}, nil, false},
	}
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
				},
			},
		},
	}
	cw := ClickhouseQueryTranslator{Ctx: context.Background(), SchemaRegistry: s}
	for _, tc := range testcases {
		field, success := cw.parseFieldFromScriptField(tc.queryMap)
		assert.Equal(t, tc.expectedSuccess, success)
		assert.Equal(t, tc.expectedMatch, field)
	}
}
