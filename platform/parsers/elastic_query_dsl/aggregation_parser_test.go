// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/testdata"
	"github.com/QuesmaOrg/quesma/platform/testdata/clients"
	dashboard_1 "github.com/QuesmaOrg/quesma/platform/testdata/dashboard-1"
	kibana_visualize "github.com/QuesmaOrg/quesma/platform/testdata/kibana-visualize"
	opensearch_visualize "github.com/QuesmaOrg/quesma/platform/testdata/opensearch-visualize"
	"github.com/stretchr/testify/assert"
	"testing"
)

const tableName = model.SingleTableNamePlaceHolder

/*
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
								"fixed_interval": "3h"
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
								"field": "order_date"
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
								"min_doc_count": 1
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
			`SELECT floor("bytes"/1782)*1782, count() FROM ` + tableName + ` ` +
				`GROUP BY floor("bytes"/1782)*1782 ` +
				`ORDER BY floor("bytes"/1782)*1782`,
			`SELECT count() FROM ` + tableName,
		},
	},
}*/

func allAggregationTests() []testdata.AggregationTestCase {
	const lowerBoundTestNr = 220
	allTests := make([]testdata.AggregationTestCase, 0, lowerBoundTestNr)

	add := func(testsToAdd []testdata.AggregationTestCase, testFilename string) {
		for i, test := range testsToAdd {
			test.TestName = fmt.Sprintf("%s(file:%s,nr:%d)", test.TestName, testFilename, i)
			allTests = append(allTests, test)
		}
	}

	add(testdata.AggregationTests, "agg_req")
	add(testdata.AggregationTests2, "agg_req_2")
	add(testdata.AggregationTestsWithDates, "dates")
	add(testdata.GrafanaAggregationTests, "grafana")
	add(testdata.KibanaSampleDataEcommerce, "kibana-sample-data-ecommerce")
	add(testdata.KibanaSampleDataFlights, "kibana-sample-data-flights")
	add(testdata.KibanaSampleDataLogs, "kibana-sample-data-logs")
	add(testdata.PipelineAggregationTests, "pipeline_agg_req")
	add(dashboard_1.AggregationTests, "dashboard-1/agg_req")
	add(kibana_visualize.AggregationTests, "kibana-visualize/agg_req")
	add(kibana_visualize.PipelineAggregationTests, "kibana-visualize/pipeline_agg_req")
	add(opensearch_visualize.AggregationTests, "opensearch-visualize/agg_req")
	add(opensearch_visualize.PipelineAggregationTests, "opensearch-visualize/pipeline_agg_req")
	add(clients.KunkkaTests, "clients/kunkka")
	add(clients.OpheliaTests, "clients/ophelia")
	add(clients.CloverTests, "clients/clover")
	add(clients.TuringTests, "clients/turing")

	return allTests
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
		Tables: map[schema.IndexName]schema.Schema{
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
	cw := ClickhouseQueryTranslator{Ctx: context.Background(), Schema: s.Tables["logs-generic-default"]}
	for _, tc := range testcases {
		field, success := cw.parseFieldFromScriptField(tc.queryMap)
		assert.Equal(t, tc.expectedSuccess, success)
		assert.Equal(t, tc.expectedMatch, field)
	}
}
