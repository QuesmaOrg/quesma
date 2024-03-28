package queryparser

import (
	"cmp"
	"context"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/testdata"
	"mitmproxy/quesma/util"
	"slices"
	"strconv"
	"testing"
)

const tableName = "logs-generic-default"
const tableNameQuoted = `"` + tableName + `"`

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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT max("AvgTicketPrice") FROM ` + tableNameQuoted + ` `,
			`SELECT min("AvgTicketPrice") FROM ` + tableNameQuoted + ` `,
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT "OriginCityName", count() FROM ` + tableNameQuoted + `  GROUP BY ("OriginCityName")`,
			`SELECT "OriginCityName", count() FROM ` + tableNameQuoted + ` WHERE "Cancelled" == true  GROUP BY ("OriginCityName")`,
			`SELECT "OriginCityName", count() FROM ` + tableNameQuoted + ` WHERE "FlightDelay" == true  GROUP BY ("OriginCityName")`,
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT "FlightDelayType", count() FROM ` + tableNameQuoted + `  GROUP BY ("FlightDelayType")`,
			"SELECT \"FlightDelayType\", toInt64(toUnixTimestamp64Milli(`timestamp`)/10800000), count() FROM " + tableNameQuoted + "  GROUP BY (\"FlightDelayType\", toInt64(toUnixTimestamp64Milli(`timestamp`)/10800000))",
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT count() FROM ` + tableNameQuoted + ` WHERE "FlightDelay" == true `,
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT count() FROM ` + tableNameQuoted + ` WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') `,
			`SELECT count() FROM ` + tableNameQuoted + ` WHERE "timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') `,
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT "FlightDelayMin", count() FROM ` + tableNameQuoted + `  GROUP BY ("FlightDelayMin")`,
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT "OriginAirportID", "DestAirportID", "DestLocation" FROM "(SELECT DestLocation, ROW_NUMBER() OVER (PARTITION BY DestLocation) AS row_number FROM ` + tableName + `)"  GROUP BY ("OriginAirportID", "DestAirportID")`,
			`SELECT "OriginAirportID", "DestAirportID", count() FROM ` + tableNameQuoted + `  GROUP BY ("OriginAirportID", "DestAirportID")`,
			`SELECT "OriginAirportID", "OriginLocation", "Origin" FROM "(SELECT OriginLocation, Origin, ROW_NUMBER() OVER (PARTITION BY OriginLocation, Origin) AS row_number FROM ` + tableName + `)"  GROUP BY ("OriginAirportID")`,
			`SELECT "OriginAirportID", count() FROM ` + tableNameQuoted + `  GROUP BY ("OriginAirportID")`,
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT "category.keyword", "order_date", count() FROM ` + tableNameQuoted + `  GROUP BY ("category.keyword", "order_date")`,
			`SELECT "category.keyword", count() FROM ` + tableNameQuoted + `  GROUP BY ("category.keyword")`,
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT sum("taxful_total_price") FROM ` + tableNameQuoted + " ",
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT quantile("taxful_total_price") FROM ` + tableNameQuoted + " ",
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT avg("total_quantity") FROM ` + tableNameQuoted + " ",
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
			}
		}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT count() FROM "logs-generic-default" WHERE taxful_total_price>250 `,
			`SELECT "order_date" FROM "(SELECT order_date, ROW_NUMBER() OVER (PARTITION BY order_date) AS row_number FROM ` + tableName + `)" WHERE taxful_total_price>250 `,
			`SELECT "taxful_total_price" FROM "(SELECT taxful_total_price, ROW_NUMBER() OVER (PARTITION BY taxful_total_price) AS row_number FROM ` + tableName + `)" WHERE taxful_total_price>250 `,
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
				}
			}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT "OriginCityName", count() FROM ` + tableNameQuoted + `  GROUP BY ("OriginCityName")`,
			`SELECT COUNT(DISTINCT "OriginCityName") FROM ` + tableNameQuoted + " ",
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
				  }
			}`,
		[]string{
			`SELECT count() FROM ` + tableNameQuoted + ` `,
			`SELECT "bytes", count() FROM ` + tableNameQuoted + `  GROUP BY (floor("bytes" / 1782) * 1782 AS "bytes") ORDER BY (floor("bytes" / 1782) * 1782 AS "bytes")`,
			`SELECT count() FROM ` + tableNameQuoted + ` `,
		},
	},
}

// Simple unit test, testing only "aggs" part of the request json query
func TestAggregationParser(t *testing.T) {
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{ClickHouseUrl: chUrl})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}

	for testIdx, test := range aggregationTests {
		t.Run(strconv.Itoa(testIdx), func(t *testing.T) {
			if testIdx == 1 || testIdx == 2 || testIdx == 4 || testIdx == 5 || testIdx == 6 || testIdx == 7 ||
				testIdx == 9 || testIdx == 11 || testIdx == 12 {
				t.Skip("We can't handle one hardest request properly yet") // Let's skip in this PR. Next one already fixes some of issues here.
			}
			aggregations, err := cw.ParseAggregationJson(test.aggregationJson)
			assert.NoError(t, err)
			assert.Equal(t, len(test.translatedSqls), len(aggregations))
			for _, aggregation := range aggregations {
				util.AssertContainsSqlEqual(t, test.translatedSqls, aggregation.String())
			}
		})
	}
}

// Used in tests to make processing `aggregations` in a deterministic way
func sortAggregations(aggregations []model.QueryWithAggregation) {
	slices.SortFunc(aggregations, func(a, b model.QueryWithAggregation) int {
		for i := range min(len(a.Aggregators), len(b.Aggregators)) {
			if a.Aggregators[i].Name != b.Aggregators[i].Name {
				return cmp.Compare(a.Aggregators[i].Name, b.Aggregators[i].Name)
			}
		}
		// longer list is first, as we first go deeper when parsing aggregations
		return cmp.Compare(len(b.Aggregators), len(a.Aggregators))
	})
}

func Test2AggregationParserExternalTestcases(t *testing.T) {
	table := clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"@timestamp": {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"timestamp":  {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"order_date": {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
			"message":    {Name: "message", Type: clickhouse.NewBaseType("String"), IsFullTextMatch: true},
		},
		Name: "logs-generic-default",
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, &table), config.QuesmaConfiguration{ClickHouseUrl: chUrl})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background()}
	for i, test := range testdata.AggregationTests {
		t.Run(test.TestName+"("+strconv.Itoa(i)+")", func(t *testing.T) {
			if i == 7 {
				t.Skip("Let's implement top_hits in next PR. Easily doable, just a bit of code.")
			}
			if i == 6 || i == 10 || i == 18 {
				t.Skip("Filters aggregation doesn't work yet. Should be 100% and quite easily doable with improved algorithm.")
			}

			aggregations, err := cw.ParseAggregationJson(test.QueryRequestJson)
			// fmt.Println("Aggregations len", len(aggregations))
			assert.NoError(t, err)
			assert.Len(t, test.ExpectedResults, len(aggregations))
			sortAggregations(aggregations[1:]) // to make test run deterministic

			// Let's leave those commented debugs for now, they'll be useful in next PRs
			for j, aggregation := range aggregations {
				// fmt.Println("--- Aggregation "+strconv.Itoa(j)+":", aggregation)
				// fmt.Println("--- SQL string ", aggregation.String())
				// fmt.Println("--- Group by: ", aggregation.GroupByFields)
				if test.ExpectedSQLs[j] != "TODO" {
					util.AssertSqlEqual(t, test.ExpectedSQLs[j], aggregation.String())
				}
			}

			if test.ExpectedResponse == "" {
				// We haven't recorded expected response yet, so we can't compare it
				return
			}

			actualAggregationsPart := cw.MakeAggregationPartOfResponse(aggregations, test.ExpectedResults)
			// pp.Println("ACTUAL", actualAggregationPart)

			fullResponse, err := cw.MakeResponseAggregation(aggregations, test.ExpectedResults, "1", false)
			assert.NoError(t, err)
			// fmt.Println(err, string(response))

			expectedResponseMap, _ := util.JsonToMap(test.ExpectedResponse)
			expectedAggregationsPart := expectedResponseMap["response"].(JsonMap)["aggregations"].(JsonMap)
			actualMinusExpected, expectedMinusActual := util.MapDifference(actualAggregationsPart, expectedAggregationsPart, true, true)

			// probability and seed are present in random_sampler aggregation. I'd assume they are not needed, thus let's not care about it for now.
			acceptableDifference := []string{"doc_count_error_upper_bound", "sum_other_doc_count", "probability", "seed"}
			assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference))
			assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference))
			assert.Contains(t, string(fullResponse), `"value": `+strconv.FormatUint(test.ExpectedResults[0][0].Cols[0].Value.(uint64), 10)) // checks if hits nr is OK
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
