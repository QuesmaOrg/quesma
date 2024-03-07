package queryparser

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/kibana"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/util"
	"regexp"
	"strconv"
	"time"
)

type JsonMap = map[string]interface{}

type ClickhouseQueryTranslator struct {
	ClickhouseLM *clickhouse.LogManager
	Table        *clickhouse.Table
}

func makeResponseSearchQueryNormal[T fmt.Stringer](ResultSet []T) ([]byte, error) {
	hits := make([]model.SearchHit, len(ResultSet))
	for i, row := range ResultSet {
		hits[i] = model.SearchHit{
			Source: []byte(row.String()),
		}
	}
	response := model.SearchResp{
		Hits: model.SearchHits{
			Hits: hits,
			Total: &model.Total{
				Value:    len(ResultSet),
				Relation: "eq",
			},
		},
	}
	return json.MarshalIndent(response, "", "  ")
}

func makeResponseSearchQueryCount[T fmt.Stringer](ResultSet []T) ([]byte, error) {
	aggregations := JsonMap{
		"suggestions": JsonMap{
			"doc_count_error_upper_bound": 0,
			"sum_other_doc_count":         0,
			"buckets":                     []interface{}{},
		},
		"unique_terms": JsonMap{
			"value": 0,
		},
	}
	response := model.SearchResp{
		Aggregations:      aggregations,
		DidTerminateEarly: new(bool), // a bit hacky with pointer, but seems like the only way https://stackoverflow.com/questions/37756236/json-golang-boolean-omitempty
		Hits: model.SearchHits{
			Hits: []model.SearchHit{},
			Total: &model.Total{
				Value:    len(ResultSet),
				Relation: "eq",
			},
		},
	}
	return json.MarshalIndent(response, "", "  ")
}

func MakeResponseSearchQuery[T fmt.Stringer](ResultSet []T, typ model.SearchQueryType) ([]byte, error) {
	switch typ {
	case model.Normal:
		return makeResponseSearchQueryNormal(ResultSet)
	case model.Count:
		return makeResponseSearchQueryCount(ResultSet)
	}
	return nil, fmt.Errorf("unknown SearchQueryType: %v", typ)
}

func makeResponseAsyncSearchAggregated(ResultSet []model.QueryResultRow, typ model.AsyncSearchQueryType) ([]byte, error) {
	buckets := make([]JsonMap, 0, len(ResultSet))
	returnedRows := 0
	for i, row := range ResultSet {
		if typ == model.AggsByField && i == 10 { // facets show only 10 top values
			break
		}
		buckets = append(buckets, make(JsonMap))
		for _, col := range row.Cols {
			buckets[i][col.ColName] = col.Value
		}
		returnedRows += int(row.Cols[model.ResultColDocCountIndex].Value.(uint64))
	}
	var sampleCount uint64 // uint64 because that's what clickhouse reader returns
	for _, row := range ResultSet {
		sampleCount += row.Cols[model.ResultColDocCountIndex].Value.(uint64)
	}

	var id *string
	aggregations := JsonMap{}
	switch typ {
	case model.Histogram:
		aggregations["0"] = JsonMap{
			"buckets": buckets,
		}
		id = new(string)
		*id = "fake-id"
	case model.AggsByField:
		aggregations["sample"] = JsonMap{
			"doc_count": int(sampleCount),
			"sample_count": JsonMap{
				"value": int(sampleCount),
			},
			"top_values": JsonMap{
				"buckets":                     buckets,
				"sum_other_doc_count":         int(sampleCount) - returnedRows,
				"doc_count_error_upper_bound": 0,
			},
		}
	default:
		return nil, fmt.Errorf("unknown AsyncSearchAggregatedQueryType: %v", typ)
	}

	response := model.AsyncSearchEntireResp{
		Response: model.SearchResp{
			Aggregations: aggregations,
			Hits: model.SearchHits{
				Hits: []model.SearchHit{}, // seems redundant, but can't remove this, created JSON won't match
				Total: &model.Total{
					Value:    int(sampleCount),
					Relation: "eq",
				},
			},
		},
		ID: id,
	}
	return json.MarshalIndent(response, "", "  ")
}

func makeResponseAsyncSearchList(ResultSet []model.QueryResultRow, typ model.AsyncSearchQueryType) ([]byte, error) {
	hits := make([]model.SearchHit, len(ResultSet))
	for i := range ResultSet {
		hits[i].Fields = make(map[string][]interface{})
		for _, col := range ResultSet[i].Cols {
			hits[i].Fields[col.ColName] = []interface{}{col.Value}
		}
	}

	var total *model.Total
	var id *string
	switch typ {
	case model.CountAsync:
		var countValue uint64
		if len(ResultSet) > 0 && len(ResultSet[0].Cols) > 0 {
			if val, ok := ResultSet[0].Cols[0].Value.(uint64); ok {
				countValue = val
			} else {
				logger.Error().Msgf("Failed extracting Count value SQL query result [%v]", ResultSet)
				countValue = 0
			}
		}
		hits = make([]model.SearchHit, 0) // need to remove count result from hits
		total = &model.Total{
			Value:    int(countValue),
			Relation: "eq",
		}
	case model.ListByField:
		total = &model.Total{
			Value:    len(ResultSet),
			Relation: "eq",
		}
	case model.ListAllFields:
		total = &model.Total{
			Value:    len(ResultSet),
			Relation: "eq",
		}
		for i := range ResultSet {
			hits[i].ID = strconv.Itoa(i + 1)
			hits[i].Index = "index-TODO-insert-tablename-index-here"
			hits[i].Score = 1
			hits[i].Version = 1
			hits[i].Sort = []any{
				"2024-01-30T19:38:54.607Z",
				2944,
			}
			hits[i].Highlight = map[string][]string{}
		}
		id = new(string)
		*id = "fake-id"
	default:
		return nil, fmt.Errorf("unknown AsyncSearchListQueryType: %v", typ)
	}

	response := model.AsyncSearchEntireResp{
		Response: model.SearchResp{
			Hits: model.SearchHits{
				Total: total,
				Hits:  hits,
			},
		},
		ID: id,
	}
	return json.MarshalIndent(response, "", "  ")
}

func makeResponseAsyncSearchEarliestLatestTimestamp(ResultSet []model.QueryResultRow) ([]byte, error) {
	var earliest, latest *time.Time = nil, nil
	if len(ResultSet) >= 1 {
		if date, ok := ResultSet[0].Cols[0].Value.(time.Time); ok {
			earliest = &date
		}
	}
	if len(ResultSet) >= 2 {
		if date, ok := ResultSet[1].Cols[0].Value.(time.Time); ok {
			latest = &date
		}
	}
	response := model.AsyncSearchEntireResp{
		Response: model.SearchResp{
			Aggregations: JsonMap{
				"earliest_timestamp": JsonMap{
					"value": earliest,
				},
				"latest_timestamp": JsonMap{
					"value": latest,
				},
			},
			Hits: model.SearchHits{
				Hits: []model.SearchHit{}, // seems redundant, but can't remove this, created JSON won't match
				Total: &model.Total{
					Value:    len(ResultSet),
					Relation: "eq",
				},
			},
		},
	}
	return json.MarshalIndent(response, "", "  ")
}

func MakeResponseAsyncSearchQuery(ResultSet []model.QueryResultRow, typ model.AsyncSearchQueryType) ([]byte, error) {
	switch typ {
	case model.Histogram, model.AggsByField:
		return makeResponseAsyncSearchAggregated(ResultSet, typ)
	case model.ListByField, model.ListAllFields:
		return makeResponseAsyncSearchList(ResultSet, typ)
	case model.EarliestLatestTimestamp:
		return makeResponseAsyncSearchEarliestLatestTimestamp(ResultSet)
	case model.CountAsync:
		return makeResponseAsyncSearchList(ResultSet, typ)
	default:
		return nil, fmt.Errorf("unknown AsyncSearchQueryType: %v", typ)
	}
}

func (cw *ClickhouseQueryTranslator) finishMakeResponse(query model.QueryWithAggregation, ResultSet []model.QueryResultRow, level int) []model.JsonMap {
	// fmt.Println("FinishMakeResponse", query, ResultSet, level, query.Type.String())
	if query.Type.IsBucketAggregation() {
		return query.Type.TranslateSqlResponseToJson(ResultSet, level)
	} else { // metrics
		lastAggregator := query.Aggregators[len(query.Aggregators)-1].Name
		return []model.JsonMap{
			{
				lastAggregator: query.Type.TranslateSqlResponseToJson(ResultSet, level)[0],
			},
		}
	}
}

// Returns if row1 and row2 have the same values for the first level + 1 fields
func (cw *ClickhouseQueryTranslator) sameGroupByFields(row1, row2 model.QueryResultRow, level int) bool {
	for i := 0; i <= level; i++ {
		if row1.Cols[i].ExtractValue() != row2.Cols[i].ExtractValue() {
			return false
		}
	}
	return true
}

// Splits ResultSet into buckets, based on the first level + 1 fields
// E.g. if level == 0, we split into buckets based on the first field,
// e.g. [row(1, ...), row(1, ...), row(2, ...), row(2, ...), row(3, ...)] -> [[row(1, ...), row(1, ...)], [row(2, ...), row(2, ...)], [row(3, ...)]]
func (cw *ClickhouseQueryTranslator) splitResultSetIntoBuckets(ResultSet []model.QueryResultRow, level int) [][]model.QueryResultRow {
	if len(ResultSet) == 0 {
		return [][]model.QueryResultRow{{}}
	}

	buckets := [][]model.QueryResultRow{{}}
	curBucket := 0
	lastRow := ResultSet[0]
	for _, row := range ResultSet {
		if cw.sameGroupByFields(row, lastRow, level) {
			buckets[curBucket] = append(buckets[curBucket], row)
		} else {
			curBucket++
			buckets = append(buckets, []model.QueryResultRow{row})
		}
		lastRow = row
	}
	return buckets
}

// DFS algorithm
// 'aggregatorsLevel' - index saying which (sub)aggregation we're handling
// 'selectLevel' - which field from select we're grouping by at current level (or not grouping by, if query.Aggregators[aggregatorsLevel].Empty == true)
func (cw *ClickhouseQueryTranslator) makeResponseAggregationRecursive(query model.QueryWithAggregation,
	ResultSet []model.QueryResultRow, aggregatorsLevel, selectLevel int) []model.JsonMap {

	// either we finish
	if aggregatorsLevel == len(query.Aggregators) || (aggregatorsLevel == len(query.Aggregators)-1 && !query.Type.IsBucketAggregation()) {
		/*
			if len(ResultSet) > 0 {
				pp.Println(query.Type, "level1: ", level1, "level2: ", level2, "cols: ", len(ResultSet[0].Cols))
			} else {
				pp.Println(query.Type, "level1: ", level1, "cols: no cols")
			}
		*/
		return cw.finishMakeResponse(query, ResultSet, selectLevel)
	}
	if len(ResultSet) == 0 {
		return nil
	}

	// fmt.Println("level1 :/", level1, " level2 B):", level2)

	// or we need to go deeper
	var bucketsReturnMap []model.JsonMap
	if query.Aggregators[aggregatorsLevel].Empty {
		bucketsReturnMap = append(bucketsReturnMap, cw.makeResponseAggregationRecursive(query, ResultSet, aggregatorsLevel+1, selectLevel)...)
	} else {
		buckets := cw.splitResultSetIntoBuckets(ResultSet, selectLevel)
		for _, bucket := range buckets {
			bucketsReturnMap = append(bucketsReturnMap, cw.makeResponseAggregationRecursive(query, bucket, aggregatorsLevel+1, selectLevel+1)...)
		}
	}

	result := make(model.JsonMap, 1)
	subResult := make(model.JsonMap, 1)

	// The if below: very hacky, but works for now. I have an idea how to fix this and make code nice, but it'll take a while to refactor.
	// Basically, for now every not-ending subaggregation has "buckets" key. Only exception is "sampler", which doesn't, thus this if.
	//
	// I'd like to keep an actual tree after the refactor, not a list of paths from root to leaf, as it is now.
	// Then in the tree (in each node) I'd remember where I am at the moment (e.g. here I'm in "sampler",
	// so I don't need buckets). It'd enable some custom handling for another weird types of requests.
	if query.Aggregators[aggregatorsLevel].Empty {
		subResult = bucketsReturnMap[0]
	} else {
		subResult["buckets"] = bucketsReturnMap
	}

	result[query.Aggregators[aggregatorsLevel].Name] = subResult
	return []model.JsonMap{result}
}

func (cw *ClickhouseQueryTranslator) MakeAggregationPartOfResponse(queries []model.QueryWithAggregation, ResultSets [][]model.QueryResultRow) model.JsonMap {
	aggregations := model.JsonMap{}
	for i, query := range queries[1:] { // first is count, we don't use that for aggregations
		aggregation := cw.makeResponseAggregationRecursive(query, ResultSets[i+1], 0, 0)[0] // result of root node is always a single map, thus [0]
		aggregations = util.MergeMaps(aggregations, aggregation)
	}
	return aggregations
}

func (cw *ClickhouseQueryTranslator) MakeResponseAggregation(queries []model.QueryWithAggregation, ResultSets [][]model.QueryResultRow) ([]byte, error) {
	response := model.AsyncSearchEntireResp{
		Response: model.SearchResp{
			Aggregations: cw.MakeAggregationPartOfResponse(queries, ResultSets),
			Hits: model.SearchHits{
				Hits: []model.SearchHit{}, // seems redundant, but can't remove this, created JSON won't match
				Total: &model.Total{
					Value:    int(ResultSets[0][0].Cols[0].Value.(uint64)), // TODO just change this to uint64? It works now.
					Relation: "eq",
				},
			},
		},
	}
	return json.MarshalIndent(response, "", "  ")
}

// GetFieldsList
// TODO flatten tuples, I think (or just don't support them for now, we don't want them at the moment in production schemas)
func (cw *ClickhouseQueryTranslator) GetFieldsList() []string {
	return []string{"message"}
}

func (cw *ClickhouseQueryTranslator) BuildSelectQuery(fields []string, whereClause string) *model.Query {
	return &model.Query{
		Fields:      fields,
		WhereClause: whereClause,
		FromClause:  cw.Table.FullTableName(),
		CanParse:    true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildSimpleSelectQuery(whereClause string) *model.Query {
	return &model.Query{
		Fields:      []string{"*"},
		WhereClause: whereClause,
		FromClause:  cw.Table.FullTableName(),
		CanParse:    true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildSimpleCountQuery(whereClause string) *model.Query {
	return &model.Query{
		NonSchemaFields: []string{"count()"},
		WhereClause:     whereClause,
		FromClause:      cw.Table.FullTableName(),
		CanParse:        true,
	}
}

// GetNMostRecentRows fieldName == "*" ==> we query all
// otherwise ==> only this 1 field
func (cw *ClickhouseQueryTranslator) BuildNMostRecentRowsQuery(fieldName, timestampFieldName, whereClause string, limit int) *model.Query {
	suffixClauses := make([]string, 0)
	if len(timestampFieldName) > 0 {
		suffixClauses = append(suffixClauses, "ORDER BY `"+timestampFieldName+"` DESC")
	}
	if limit > 0 {
		suffixClauses = append(suffixClauses, "LIMIT "+strconv.Itoa(limit))
	}
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{},
		WhereClause:     whereClause,
		SuffixClauses:   suffixClauses,
		FromClause:      cw.Table.FullTableName(),
		CanParse:        true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildAutocompleteQuery(fieldName, whereClause string, limit int) *model.Query {
	suffixClauses := make([]string, 0)
	if limit > 0 {
		suffixClauses = append(suffixClauses, "LIMIT "+strconv.Itoa(limit))
	}
	return &model.Query{
		IsDistinct:      true,
		Fields:          []string{fieldName},
		NonSchemaFields: []string{},
		WhereClause:     whereClause,
		SuffixClauses:   suffixClauses,
		FromClause:      cw.Table.FullTableName(),
		CanParse:        true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildHistogramQuery(timestampFieldName, whereClauseOriginal, fixedInterval string) (*model.Query, time.Duration) {
	histogramOneBar, err := kibana.ParseInterval(fixedInterval)
	if err != nil {
		panic(err)
	}
	groupByClause := clickhouse.TimestampGroupBy(timestampFieldName, cw.Table.GetDateTimeType(timestampFieldName), histogramOneBar)
	// [WARNING] This is a little oversimplified, but it seems to be good enough for now (==satisfies Kibana's histogram)
	//
	// In Elasticsearch's `date_histogram` aggregation implementation, the timestamps for the intervals are generated independently of the document data.
	// The aggregation divides the specified time range into intervals based on the interval unit (e.g., minute, hour, day) and generates timestamps for each interval,
	// irrespective of the actual timestamps of the documents.
	query := model.Query{
		Fields:          []string{},
		NonSchemaFields: []string{groupByClause, "count()"},
		WhereClause:     whereClauseOriginal,
		SuffixClauses:   []string{"GROUP BY " + groupByClause},
		FromClause:      cw.Table.FullTableName(),
		CanParse:        true,
	}
	return &query, histogramOneBar
}

//lint:ignore U1000 Not used yet
func (cw *ClickhouseQueryTranslator) BuildAutocompleteSuggestionsQuery(fieldName string, prefix string, limit int) *model.Query {
	whereClause := ""
	if len(prefix) > 0 {
		whereClause = strconv.Quote(fieldName) + " iLIKE '" + prefix + "%'"
	}
	suffixClauses := make([]string, 0)
	if limit > 0 {
		suffixClauses = append(suffixClauses, "LIMIT "+strconv.Itoa(limit))
	}
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{},
		WhereClause:     whereClause,
		SuffixClauses:   suffixClauses,
		FromClause:      cw.Table.FullTableName(),
		CanParse:        true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildFacetsQuery(fieldName, whereClause string, limit int) *model.Query {
	suffixClauses := []string{"GROUP BY " + strconv.Quote(fieldName), "ORDER BY count() DESC"}
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{"count()"},
		WhereClause:     whereClause,
		SuffixClauses:   suffixClauses,
		FromClause:      cw.Table.FullTableName(),
		CanParse:        true,
	}
}

// earliest == true  <==> we want earliest timestamp
// earliest == false <==> we want latest timestamp
func (cw *ClickhouseQueryTranslator) BuildTimestampQuery(timestampFieldName, whereClause string, earliest bool) *model.Query {
	var orderBy string
	if earliest {
		orderBy = "ORDER BY `" + timestampFieldName + "` ASC"
	} else {
		orderBy = "ORDER BY `" + timestampFieldName + "` DESC"
	}
	suffixClauses := []string{orderBy, "LIMIT 1"}
	return &model.Query{
		Fields:        []string{timestampFieldName},
		WhereClause:   whereClause,
		SuffixClauses: suffixClauses,
		FromClause:    cw.Table.FullTableName(),
		CanParse:      true,
	}
}

func (cw *ClickhouseQueryTranslator) createHistogramPartOfQuery(queryMap QueryMap) string {
	fieldName := queryMap["field"].(string)
	interval, err := kibana.ParseInterval(cw.extractInterval(queryMap))
	if err != nil {
		logger.Error().Msg(err.Error())
	}
	dateTimeType := cw.Table.GetDateTimeType(fieldName)
	if dateTimeType == clickhouse.Invalid {
		logger.Error().Msgf("Invalid date type for field %v", fieldName)
		dateTimeType = clickhouse.DateTime64
	}
	return clickhouse.TimestampGroupBy(fieldName, dateTimeType, interval)
}

var fromRegexp = regexp.MustCompile(`>=?parseDateTime64BestEffort\('([^']+)'\)`)
var toRegexp = regexp.MustCompile(`<=?parseDateTime64BestEffort\('([^']+)'\)`)

func durationFromWhere(input string) (time.Duration, error) {
	fromMatch := fromRegexp.FindAllStringSubmatch(input, -1)
	toMatch := toRegexp.FindAllStringSubmatch(input, -1)
	if len(fromMatch) < 1 || len(toMatch) < 1 {
		return 0, errors.New("date match failed")
	}
	from := fromMatch[0]
	to := toMatch[0]

	startTime, err := time.Parse(time.RFC3339, from[1])
	if err != nil {
		return 0, err
	}

	endTime, err := time.Parse(time.RFC3339, to[1])
	if err != nil {
		return 0, err
	}

	return endTime.Sub(startTime), nil
}
