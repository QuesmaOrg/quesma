package queryparser

import (
	"github.com/stretchr/testify/require"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/testdata"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var chUrl, _ = url.Parse("")

// TODO:
//  1. 14th test, "Query string". "(message LIKE '%%%' OR message LIKE '%logged%')", is it really
//     what should be? According to docs, I think so... Maybe test in Kibana?
//     OK, Kibana disagrees, it is indeed wrong.
func TestQueryParserStringAttrConfig(t *testing.T) {
	tableName := "logs-generic-default"
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "@timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}

	lm := clickhouse.NewEmptyLogManager(config.QuesmaConfiguration{ClickHouseUrl: chUrl})
	lm.AddTableIfDoesntExist(table)

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}
	for _, tt := range testdata.TestsSearch {
		t.Run(tt.Name, func(t *testing.T) {
			simpleQuery, queryType := cw.ParseQuery(tt.QueryJson)
			assert.True(t, simpleQuery.CanParse, "can parse")
			assert.Contains(t, tt.WantedSql, simpleQuery.Sql.Stmt, "contains wanted sql")
			assert.Equal(t, tt.WantedQueryType, queryType, "equals to wanted query type")

			query := cw.BuildSimpleSelectQuery(simpleQuery.Sql.Stmt)
			assert.Contains(t, tt.WantedQuery, *query)
		})
	}
}

// TODO this test gives wrong results??
func TestQueryParserNoAttrsConfig(t *testing.T) {
	tableName := "logs-generic-default"
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "@timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewChTableConfigNoAttrs(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{ClickHouseUrl: chUrl})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}
	for _, tt := range testdata.TestsSearchNoAttrs {
		t.Run(tt.Name, func(t *testing.T) {
			simpleQuery, queryType := cw.ParseQuery(tt.QueryJson)
			assert.True(t, simpleQuery.CanParse)
			assert.Contains(t, tt.WantedSql, simpleQuery.Sql.Stmt)
			assert.Equal(t, tt.WantedQueryType, queryType)

			query := cw.BuildSimpleSelectQuery(simpleQuery.Sql.Stmt)
			assert.Contains(t, tt.WantedQuery, *query)
		})
	}
}

// TODO this will be updated in the next PR
var tests = []string{
	`{
		"_source": {
			"excludes": []
		},
		"aggs": {
			"0": {
				"histogram": {
					"field": "FlightDelayMin",
					"interval": 1,
					"min_doc_count": 1
				}
			}
		},
		"fields": [
			{
				"field": "timestamp",
				"format": "date_time"
			}
		],
		"query": {
			"bool": {
				"filter": [
					{
						"range": {
							"timestamp": {
								"format": "strict_date_optional_time",
								"gte": "2024-02-02T13:47:16.029Z",
								"lte": "2024-02-09T13:47:16.029Z"
							}
						}
					}
				],
				"must": [],
				"must_not": [
					{
						"match_phrase": {
							"FlightDelayMin": {
								"query": 0
							}
						}
					}
				],
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
	`{
		"_source": {
			"excludes": []
		},
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
		"fields": [
			{
				"field": "timestamp",
				"format": "date_time"
			}
		],
		"query": {
			"bool": {
				"filter": [
					{
						"range": {
							"timestamp": {
								"format": "strict_date_optional_time",
								"gte": "2024-02-02T13:47:16.029Z",
								"lte": "2024-02-09T13:47:16.029Z"
							}
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
}

// TODO this will be updated in the next PR
func TestNew(t *testing.T) {
	tableName := `"logs-generic-default"`
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{ClickHouseUrl: chUrl})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}
	for _, tt := range tests {
		t.Run("test", func(t *testing.T) {
			simpleQuery, _ := cw.ParseQueryAsyncSearch(tt)
			assert.True(t, simpleQuery.CanParse)
		})
	}
}

// Test_parseRange tests if DateTime64 field properly uses Clickhouse's 'parseDateTime64BestEffort' function
func Test_parseRange_DateTime64(t *testing.T) {
	rangePartOfQuery := QueryMap{
		"timestamp": QueryMap{
			"format": "strict_date_optional_time",
			"gte":    "2024-02-02T13:47:16.029Z",
			"lte":    "2024-02-09T13:47:16.029Z",
		},
	}
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{ClickHouseUrl: chUrl})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}

	whereClause := cw.parseRange(rangePartOfQuery).Sql.Stmt
	split := strings.Split(whereClause, "parseDateTime64BestEffort")
	assert.Len(t, split, 3)
}

// Test_parseRange tests if DateTime field properly uses Clickhouse's 'parseDateTimeBestEffort' function
func Test_parseRange_DateTime(t *testing.T) {
	rangePartOfQuery := QueryMap{
		"timestamp": QueryMap{
			"format": "strict_date_optional_time",
			"gte":    "2024-02-02T13:47:16.029Z",
			"lte":    "2024-02-09T13:47:16.029Z",
		},
	}
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{ClickHouseUrl: chUrl})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}

	whereClause := cw.parseRange(rangePartOfQuery).Sql.Stmt
	split := strings.Split(whereClause, "parseDateTimeBestEffort")
	assert.Len(t, split, 3)
}

func TestFilterNonEmpty(t *testing.T) {
	tests := []struct {
		array    []Statement
		filtered []Statement
	}{
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement("")},
			[]Statement{},
		},
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement("a"), NewCompoundStatement("")},
			[]Statement{NewSimpleStatement("a")},
		},
		{
			[]Statement{NewCompoundStatement("a"), NewSimpleStatement("b"), NewCompoundStatement("c")},
			[]Statement{NewCompoundStatement("a"), NewSimpleStatement("b"), NewCompoundStatement("c")},
		},
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, tt.filtered, filterNonEmpty(tt.array))
		})
	}
}

func TestOrAndAnd(t *testing.T) {
	tests := []struct {
		stmts []Statement
		want  Statement
	}{
		{
			[]Statement{NewSimpleStatement("a"), NewSimpleStatement("b"), NewSimpleStatement("c")},
			NewCompoundStatement("a AND b AND c"),
		},
		{
			[]Statement{NewSimpleStatement("a"), NewSimpleStatement(""), NewCompoundStatement(""), NewCompoundStatement("b")},
			NewCompoundStatement("a AND (b)"),
		},
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement(""), NewSimpleStatement("a"), NewCompoundStatement(""), NewSimpleStatement(""), NewCompoundStatement("")},
			NewSimpleStatement("a"),
		},
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement(""), NewSimpleStatement(""), NewSimpleStatement("")},
			NewSimpleStatement(""),
		},
		{
			[]Statement{NewCompoundStatement("a AND b"), NewCompoundStatement("c AND d"), NewCompoundStatement("e AND f")},
			NewCompoundStatement("(a AND b) AND (c AND d) AND (e AND f)"),
		},
	}
	// copy, because and() and or() modify the slice
	for i, tt := range tests {
		t.Run("AND "+strconv.Itoa(i), func(t *testing.T) {
			b := make([]Statement, len(tt.stmts))
			copy(b, tt.stmts)
			assert.Equal(t, tt.want, and(b))
		})
	}
	for i, tt := range tests {
		t.Run("OR "+strconv.Itoa(i), func(t *testing.T) {
			tt.want.Stmt = strings.ReplaceAll(tt.want.Stmt, "AND", "OR")
			for i := range tt.stmts {
				tt.stmts[i].Stmt = strings.ReplaceAll(tt.stmts[i].Stmt, "AND", "OR")
			}
			assert.Equal(t, tt.want, or(tt.stmts))
		})
	}
}

func TestQueryParserTimeUnit(t *testing.T) {
	unitMappings := map[string]string{
		"m": "minute",
		"s": "second",
		"h": "hour",
		"H": "hour",
		"d": "day",
		"w": "week",
		"M": "month",
		"y": "year",
	}
	for inputUnit, expectedUnit := range unitMappings {
		unit, err := parseTimeUnit(inputUnit)
		require.NoError(t, err)
		assert.Equal(t, expectedUnit, unit)
	}
	_, err := parseTimeUnit("unknown")
	require.Error(t, err)
}

func TestQueryDateMathExpressionTokenizer(t *testing.T) {
	exprs := map[string][]string{
		"now-15m":     {"now", "-", "15", "m"},
		"now-15m-25s": {"now", "-", "15", "m", "-", "25", "s"},
		"no":          {},
		"noy":         {},
		"now*":        {"now"},
		"now+a":       {"now", "+"},
		"now+5QQ":     {"now", "+", "5", "Q"},
	}
	for expr, expectedTokens := range exprs {
		tokens := tokenizeDateMathExpr(expr)
		assert.Equal(t, len(expectedTokens), len(tokens))
		assert.Equal(t, expectedTokens, tokens)
	}
}

func TestQueryParseDateMathExpression(t *testing.T) {
	exprs := map[string]string{
		"now-15m":    "subDate(now(), INTERVAL 15 minute)",
		"now-15m+5s": "addDate(subDate(now(), INTERVAL 15 minute), INTERVAL 5 second)",
		"now-":       "now()",
		"now-15m+":   "subDate(now(), INTERVAL 15 minute)",
	}
	for expr, expected := range exprs {
		resultExpr := parseDateMathExpression(expr)
		assert.Equal(t, expected, resultExpr)
	}
}
