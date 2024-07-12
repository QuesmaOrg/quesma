package queryparser

import (
	"context"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/model"
	"quesma/queryparser/query_util"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/testdata/query_optimizers"
	"quesma/util"
	"strconv"
	"strings"
	"testing"
)

func TestMergeMetricsAggsTransformer(t *testing.T) {
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

	s := staticRegistry{
		tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.TypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.TypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.TypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.TypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.TypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.TypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.TypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.TypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.TypeText},
				},
			},
		},
	}
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), SchemaRegistry: s}
	for i, test := range query_optimizers.MergeMetricsAggsOptimizerTests {
		t.Run(test.TestName+"("+strconv.Itoa(i)+")", func(t *testing.T) {
			if test.TestName == "Max/Sum bucket with some null buckets. Reproduce: Visualize -> Vertical Bar: Metrics: Max (Sum) Bucket (Aggregation: Date Histogram, Metric: Min)" {
				t.Skip("Needs to be fixed by keeping last key for every aggregation. Now we sometimes don't know it. Hard to reproduce, leaving it for separate PR")
			}
			if test.TestName == "complex sum_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Sum Bucket (Bucket: Date Histogram, Metric: Average), Buckets: X-Asis: Histogram" {
				t.Skip("Waiting for fix. Now we handle only the case where pipeline agg is at the same nesting level as its parent. Should be quick to fix.")
			}
			if i == 27 || i == 29 || i == 30 {
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
			if test.TestName == "it's the same input as in previous test, but with the original output from Elastic."+
				"Skipped for now, as our response is different in 2 things: key_as_string date (probably not important) + we don't return 0's (e.g. doc_count: 0)."+
				"If we need clients/kunkka/test_0, used to be broken before aggregations merge fix" {
				t.Skip("Unskip and remove the previous test after those fixes.")
			}
			if test.TestName == "clients/kunkka/test_1, used to be broken before aggregations merge fix" {
				t.Skip("Small details left for this test to be correct. I'll (Krzysiek) fix soon after returning to work")
			}

			body, parseErr := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, parseErr)

			queries_before_optimization, canParse, err := cw.ParseQuery(body)
			assert.True(t, canParse)
			assert.NoError(t, err)

			queries, err := MergeMetricsAggsTransformer{}.Transform(queries_before_optimization)
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
			acceptableDifference := []string{"doc_count_error_upper_bound", "sum_other_doc_count", "probability", "seed", "bg_count", "doc_count", model.KeyAddedByQuesma}
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
