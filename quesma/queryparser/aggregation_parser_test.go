package queryparser

import (
	"cmp"
	"context"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/query_util"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/testdata"
	dashboard_1 "mitmproxy/quesma/testdata/dashboard-1"
	kibana_visualize "mitmproxy/quesma/testdata/kibana-visualize"
	opensearch_visualize "mitmproxy/quesma/testdata/opensearch-visualize"
	"mitmproxy/quesma/util"
	"slices"
	"strconv"
	"strings"
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
			},
			"size": 0
		}`,
		[]string{
			`SELECT maxOrNull("AvgTicketPrice") FROM ` + tableNameQuoted,
			`SELECT minOrNull("AvgTicketPrice") FROM ` + tableNameQuoted,
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
			`SELECT "OriginCityName", count() FROM ` + tableNameQuoted + ` GROUP BY "OriginCityName" ORDER BY "OriginCityName"`,
			`SELECT "OriginCityName", count() FROM ` + tableNameQuoted + ` WHERE "Cancelled"==true GROUP BY "OriginCityName" ORDER BY "OriginCityName"`,
			`SELECT "OriginCityName", count() FROM ` + tableNameQuoted + ` WHERE "FlightDelay"==true GROUP BY "OriginCityName" ORDER BY "OriginCityName"`,
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
			`SELECT "FlightDelayType", count() FROM ` + tableNameQuoted + ` GROUP BY "FlightDelayType" ORDER BY "FlightDelayType"`,
			`SELECT "FlightDelayType", toInt64(toUnixTimestamp64Milli("timestamp") / 10800000), count() ` +
				`FROM ` + tableNameQuoted + " " +
				`GROUP BY "FlightDelayType", toInt64(toUnixTimestamp64Milli("timestamp") / 10800000) ` +
				`ORDER BY "FlightDelayType", toInt64(toUnixTimestamp64Milli("timestamp") / 10800000)`,
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
			`SELECT count() FROM ` + tableNameQuoted + ` WHERE "FlightDelay"==true`,
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
			`SELECT count() FROM ` + tableNameQuoted + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT count() FROM ` + tableNameQuoted + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') ` +
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
			`SELECT "FlightDelayMin", count() FROM ` + tableNameQuoted + ` GROUP BY "FlightDelayMin" ORDER BY "FlightDelayMin"`,
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
			`SELECT "OriginAirportID", "DestAirportID", count() FROM ` + tableNameQuoted + ` ` +
				`GROUP BY "OriginAirportID", "DestAirportID" ORDER BY "OriginAirportID", "DestAirportID"`,
			`SELECT "OriginAirportID", "DestAirportID", "DestLocation" ` +
				`FROM (SELECT "DestLocation", ROW_NUMBER() ` +
				`OVER (PARTITION BY "OriginAirportID", "DestAirportID"  ) AS row_number ` +
				`FROM "logs-generic-default") ` +
				`WHERE "row_number"<=1 ` +
				`GROUP BY "OriginAirportID", "DestAirportID" ` +
				`ORDER BY "OriginAirportID", "DestAirportID"`,
			`SELECT "OriginAirportID", "OriginLocation", "Origin" ` +
				`FROM (SELECT "OriginLocation", "Origin", ROW_NUMBER() ` +
				`OVER (PARTITION BY "OriginAirportID"  ) AS row_number ` +
				`FROM "logs-generic-default") ` +
				`WHERE "row_number"<=1 ` +
				`GROUP BY "OriginAirportID" ` +
				`ORDER BY "OriginAirportID"`,
			`SELECT "OriginAirportID", count() FROM ` + tableNameQuoted + ` GROUP BY "OriginAirportID" ORDER BY "OriginAirportID"`,
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
			`SELECT "category", toInt64(toUnixTimestamp64Milli("order_date") / 86400000), count() ` +
				`FROM ` + tableNameQuoted + ` ` +
				`GROUP BY "category", toInt64(toUnixTimestamp64Milli("order_date") / 86400000) ` +
				`ORDER BY "category", toInt64(toUnixTimestamp64Milli("order_date") / 86400000)`,
			`SELECT "category", count() FROM ` + tableNameQuoted + ` GROUP BY "category" ORDER BY "category"`,
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
			`SELECT sumOrNull("taxful_total_price") FROM ` + tableNameQuoted,
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
			`SELECT quantiles(0.500000)("taxful_total_price") AS "quantile_50" FROM ` + tableNameQuoted,
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
			`SELECT avgOrNull("total_quantity") FROM ` + tableNameQuoted,
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
			`SELECT count() FROM ` + tableNameQuoted + ` WHERE "taxful_total_price" > '250'`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000), ` +
				`maxOrNull("order_date") AS "windowed_order_date", maxOrNull("order_date") AS "windowed_order_date" ` +
				`FROM (SELECT "order_date", "order_date", ROW_NUMBER() OVER ` +
				`(PARTITION BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY "order_date" ASC ) AS row_number ` +
				`FROM ` + tableNameQuoted + ` ` +
				`WHERE "taxful_total_price" > '250') ` +
				`WHERE ("taxful_total_price" > '250' AND "row_number"<=10) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000), ` +
				`maxOrNull("taxful_total_price") AS "windowed_taxful_total_price", maxOrNull("order_date") AS "windowed_order_date" ` +
				`FROM (SELECT "taxful_total_price", "order_date", ROW_NUMBER() OVER ` +
				`(PARTITION BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY "order_date" ASC ) AS row_number ` +
				`FROM ` + tableNameQuoted + ` ` +
				`WHERE "taxful_total_price" > '250') ` +
				`WHERE ("taxful_total_price" > '250' AND "row_number"<=10) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000), count() ` +
				`FROM ` + tableNameQuoted + ` ` +
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
			`SELECT "OriginCityName", count() FROM ` + tableNameQuoted + ` GROUP BY "OriginCityName" ORDER BY count() DESC LIMIT 10`,
			`SELECT count(DISTINCT "OriginCityName") FROM ` + tableNameQuoted,
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
			`SELECT floor("bytes"/1782.000000)*1782.000000, count() FROM ` + tableNameQuoted + ` ` +
				`GROUP BY floor("bytes"/1782.000000)*1782.000000 ` +
				`ORDER BY floor("bytes"/1782.000000)*1782.000000`,
			`SELECT count() FROM ` + tableNameQuoted,
		},
	},
}

// Simple unit test, testing only "aggs" part of the request json query
func TestAggregationParser(t *testing.T) {
	// logger.InitSimpleLoggerForTests() FIXME there are 2 warns if you enable them, might look into that
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}

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

func Test2AggregationParserExternalTestcases(t *testing.T) {
	// logger.InitSimpleLoggerForTests()
	table := clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"@timestamp":  {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"timestamp":   {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"order_date":  {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
			"message":     {Name: "message", Type: clickhouse.NewBaseType("String"), IsFullTextMatch: true},
			"bytes_gauge": {Name: "bytes_gauge", Type: clickhouse.NewBaseType("UInt64")},
		},
		Name:   "logs-generic-default",
		Config: clickhouse.NewDefaultCHConfig(),
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, &table), config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background()}
	allTests := testdata.AggregationTests
	allTests = append(allTests, opensearch_visualize.AggregationTests...)
	allTests = append(allTests, dashboard_1.AggregationTests...)
	allTests = append(allTests, testdata.PipelineAggregationTests...)
	allTests = append(allTests, opensearch_visualize.PipelineAggregationTests...)
	allTests = append(allTests, kibana_visualize.AggregationTests...)
	for i, test := range allTests {
		t.Run(test.TestName+"("+strconv.Itoa(i)+")", func(t *testing.T) {
			if i == 0 || i == 3 {
				t.Skip("Will work after track_total_hits")
			}
			if test.TestName == "Max/Sum bucket with some null buckets. Reproduce: Visualize -> Vertical Bar: Metrics: Max (Sum) Bucket (Aggregation: Date Histogram, Metric: Min)" {
				t.Skip("Needs to be fixed by keeping last key for every aggregation. Now we sometimes don't know it. Hard to reproduce, leaving it for separate PR")
			}
			if test.TestName == "complex sum_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Sum Bucket (Bucket: Date Histogram, Metric: Average), Buckets: X-Asis: Histogram" {
				t.Skip("Waiting for fix. Now we handle only the case where pipeline agg is at the same nesting level as its parent. Should be quick to fix.")
			}
			if i > 26 && i <= 30 {
				t.Skip("New tests, harder, failing for now. Fixes for them in 2 next PRs")
			}
			if strings.HasPrefix(test.TestName, "dashboard-1") {
				t.Skip("Those 2 tests have nested histograms with min_doc_count=0. I'll add support for that in next PR, already most of work done")
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

			body, parseErr := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, parseErr)

			queries, canParse, err := cw.ParseQuery(body)
			assert.True(t, canParse)
			assert.NoError(t, err)
			assert.Len(t, test.ExpectedResults, len(queries))
			sortAggregations(queries) // to make test runs deterministic

			// Let's leave those commented debugs for now, they'll be useful in next PRs
			for j, query := range queries {
				// fmt.Printf("--- Aggregation %d: %+v\n\n---SQL string: %s\n\n", j, query, query.String(context.Background()))
				if test.ExpectedSQLs[j] != "NoDBQuery" {
					util.AssertSqlEqual(t, test.ExpectedSQLs[j], query.SelectCommand.String())
				}
				if query_util.IsNonAggregationQuery(query) {
					continue
				}
				test.ExpectedResults[j] = query.Type.PostprocessResults(test.ExpectedResults[j])
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
			actualMinusExpected, expectedMinusActual := util.MapDifference(response.Aggregations, expectedAggregationsPart, true, true)

			// probability and seed are present in random_sampler aggregation. I'd assume they are not needed, thus let's not care about it for now.
			acceptableDifference := []string{"doc_count_error_upper_bound", "sum_other_doc_count", "probability", "seed", "bg_count", "doc_count"}
			// pp.Println("ACTUAL", actualMinusExpected)
			// pp.Println("EXPECTED", expectedMinusActual)
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
	cw := ClickhouseQueryTranslator{Ctx: context.Background()}
	for _, tc := range testcases {
		field, success := cw.parseFieldFromScriptField(tc.queryMap)
		assert.Equal(t, tc.expectedSuccess, success)
		assert.Equal(t, tc.expectedMatch, field)
	}
}
