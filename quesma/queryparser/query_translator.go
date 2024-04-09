package queryparser

import (
	"context"
	"errors"
	"fmt"
	"math"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/kibana"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/util"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const facetsSampleSize = "20000"

type JsonMap = map[string]interface{}

type ClickhouseQueryTranslator struct {
	ClickhouseLM      *clickhouse.LogManager
	Table             *clickhouse.Table
	tokensToHighlight []string
	Ctx               context.Context
}

var completionStatusOK = func() *int { value := 200; return &value }()

func (cw *ClickhouseQueryTranslator) AddTokenToHighlight(token any) {

	if token == nil {
		return
	}

	// this logic is taken from `sprint` function
	switch token := token.(type) {
	case string:
		cw.tokensToHighlight = append(cw.tokensToHighlight, token)
	case *string:
		cw.tokensToHighlight = append(cw.tokensToHighlight, *token)
	case QueryMap:
		value := token["value"]
		cw.AddTokenToHighlight(value)
	}

}

func (cw *ClickhouseQueryTranslator) ClearTokensToHighlight() {
	cw.tokensToHighlight = []string{}
}

func makeSearchResponseNormal(ResultSet []model.QueryResultRow) *model.SearchResp {
	hits := make([]model.SearchHit, len(ResultSet))
	for i, row := range ResultSet {
		hits[i] = model.SearchHit{
			Index:  row.Index,
			Source: []byte(row.String()),
		}
	}

	return &model.SearchResp{
		Hits: model.SearchHits{
			Hits: hits,
			Total: &model.Total{
				Value:    len(ResultSet),
				Relation: "eq",
			},
		},
	}
}

func EmptySearchResponse() []byte {
	response := makeSearchResponseNormal([]model.QueryResultRow{})
	marshalled, _ := response.Marshal() // error will never happen here
	return marshalled
}

func EmptyAsyncSearchResponse(id string) []byte {
	searchResp := makeSearchResponseNormal([]model.QueryResultRow{})
	asyncSearchResp := SearchToAsyncSearchResponse(searchResp, id, false)
	marshalled, _ := asyncSearchResp.Marshal() // error will never happen here
	return marshalled
}

func (cw *ClickhouseQueryTranslator) MakeSearchResponse(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter Highlighter) (*model.SearchResp, error) {
	switch typ {
	case model.Normal:
		return makeSearchResponseNormal(ResultSet), nil
	case model.Facets, model.FacetsNumeric:
		return cw.makeSearchResponseFacets(ResultSet, typ), nil
	case model.ListByField, model.ListAllFields:
		return cw.makeSearchResponseList(ResultSet, typ, highlighter), nil
	default:
		return nil, fmt.Errorf("unknown SearchQueryType: %v", typ)
	}
}

func (cw *ClickhouseQueryTranslator) MakeSearchResponseMarshalled(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter Highlighter) ([]byte, error) {
	response, err := cw.MakeSearchResponse(ResultSet, typ, highlighter)
	if err != nil {
		return nil, err
	}
	return response.Marshal()
}

func (cw *ClickhouseQueryTranslator) makeSearchResponseFacets(ResultSet []model.QueryResultRow, typ model.SearchQueryType) *model.SearchResp {
	const maxFacets = 10 // facets show only top 10 values
	bucketsNr := min(len(ResultSet), maxFacets)
	buckets := make([]JsonMap, 0, bucketsNr)
	returnedRowsNr := 0
	var sampleCount uint64

	// Let's make the following branching only for tests' sake. In production, we always have uint64,
	// but go-sqlmock can only return int64, so let's keep it like this for now.
	// Normally, only 'uint64' case would be needed.
	if bucketsNr > 0 {
		switch ResultSet[0].Cols[model.ResultColDocCountIndex].Value.(type) {
		case int64:
			for i, row := range ResultSet[:bucketsNr] {
				buckets = append(buckets, make(JsonMap))
				for _, col := range row.Cols {
					buckets[i][col.ColName] = col.Value
				}
				returnedRowsNr += int(row.Cols[model.ResultColDocCountIndex].Value.(int64))
			}
			for _, row := range ResultSet {
				sampleCount += uint64(row.Cols[model.ResultColDocCountIndex].Value.(int64))
			}
		case uint64:
			for i, row := range ResultSet[:bucketsNr] {
				buckets = append(buckets, make(JsonMap))
				for _, col := range row.Cols {
					buckets[i][col.ColName] = col.Value
				}
				returnedRowsNr += int(row.Cols[model.ResultColDocCountIndex].Value.(uint64))
			}
			for _, row := range ResultSet {
				sampleCount += row.Cols[model.ResultColDocCountIndex].Value.(uint64)
			}
		}
	}

	aggregations := JsonMap{
		"sample": JsonMap{
			"doc_count": int(sampleCount),
			"sample_count": JsonMap{
				"value": int(sampleCount),
			},
			"top_values": JsonMap{
				"buckets":                     buckets,
				"sum_other_doc_count":         int(sampleCount) - returnedRowsNr,
				"doc_count_error_upper_bound": 0,
			},
		},
	}

	if typ == model.FacetsNumeric {
		if len(ResultSet) == 0 {
			aggregations["sample"].(JsonMap)["min_value"] = nil
			aggregations["sample"].(JsonMap)["max_value"] = nil
		} else {
			switch ResultSet[0].Cols[model.ResultColKeyIndex].Value.(type) {
			case int64, uint64, *int64, *uint64:
				var minValue, maxValue = int64(math.MaxInt), int64(math.MinInt)
				for _, row := range ResultSet {
					value := util.ExtractInt64(row.Cols[model.ResultColKeyIndex].Value)
					maxValue = max(maxValue, value)
					minValue = min(minValue, value)
				}
				aggregations["sample"].(JsonMap)["min_value"] = JsonMap{"value": minValue}
				aggregations["sample"].(JsonMap)["max_value"] = JsonMap{"value": maxValue}
			case float64, *float64:
				var minValue, maxValue = math.MaxFloat64, -math.MaxFloat64
				for _, row := range ResultSet {
					value := util.ExtractFloat64(row.Cols[model.ResultColKeyIndex].Value)
					maxValue = max(maxValue, value)
					minValue = min(minValue, value)
				}
				aggregations["sample"].(JsonMap)["min_value"] = JsonMap{"value": minValue}
				aggregations["sample"].(JsonMap)["max_value"] = JsonMap{"value": maxValue}
			}
		}
	}

	return &model.SearchResp{
		Aggregations: aggregations,
		Hits: model.SearchHits{
			Hits: []model.SearchHit{}, // seems redundant, but can't remove this, created JSON won't match
			Total: &model.Total{
				Value:    int(sampleCount),
				Relation: "eq",
			},
		},
	}
}

func (cw *ClickhouseQueryTranslator) makeSearchResponseList(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter Highlighter) *model.SearchResp {
	hits := make([]model.SearchHit, len(ResultSet))
	for i := range ResultSet {
		hits[i].Fields = make(map[string][]interface{})
		hits[i].Highlight = make(map[string][]string)
		if typ == model.ListAllFields {
			hits[i].ID = strconv.Itoa(i + 1)
			hits[i].Index = cw.Table.Name
			hits[i].Score = 1
			hits[i].Version = 1
			hits[i].Sort = []any{
				"2024-01-30T19:38:54.607Z",
				2944,
			}
		}

		for _, col := range ResultSet[i].Cols {

			hits[i].Fields[col.ColName] = []interface{}{col.Value}

			if highlighter.ShouldHighlight(col.ColName) {
				// check if we have a string here and if so, highlight it
				switch valueAsString := col.Value.(type) {
				case string:
					hits[i].Highlight[col.ColName] = highlighter.HighlightValue(valueAsString)
				case *string:
					if valueAsString != nil {
						hits[i].Highlight[col.ColName] = highlighter.HighlightValue(*valueAsString)
					}
				}
			}
		}

		// TODO: highlight and field checks
		for _, alias := range cw.Table.AliasList() {
			if v, ok := hits[i].Fields[alias.TargetFieldName]; ok {
				hits[i].Fields[alias.SourceFieldName] = v
			}
		}
	}

	return &model.SearchResp{
		Hits: model.SearchHits{
			Total: &model.Total{
				Value:    len(ResultSet),
				Relation: "eq",
			},
			Hits: hits,
		},
	}
}

func (cw *ClickhouseQueryTranslator) MakeAsyncSearchResponse(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter Highlighter, asyncRequestIdStr string, isPartial bool) (*model.AsyncSearchEntireResp, error) {
	searchResponse, err := cw.MakeSearchResponse(ResultSet, typ, highlighter)
	if err != nil {
		return nil, err
	}
	id := new(string)
	*id = asyncRequestIdStr
	response := model.AsyncSearchEntireResp{
		Response:  *searchResponse,
		ID:        id,
		IsPartial: isPartial,
		IsRunning: isPartial,
	}
	if !isPartial {
		response.CompletionStatus = completionStatusOK
	}
	return &response, nil
}

func (cw *ClickhouseQueryTranslator) MakeAsyncSearchResponseMarshalled(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter Highlighter, asyncRequestIdStr string, isPartial bool) ([]byte, error) {
	response, err := cw.MakeAsyncSearchResponse(ResultSet, typ, highlighter, asyncRequestIdStr, isPartial)
	if err != nil {
		return nil, err
	}
	return response.Marshal()
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
	const aggregation_start_index = 1
	aggregations := model.JsonMap{}
	if len(queries) <= aggregation_start_index {
		return aggregations
	}
	for i, query := range queries[aggregation_start_index:] {
		if len(ResultSets) <= i+1 {
			continue
		}
		aggregation := cw.makeResponseAggregationRecursive(query, ResultSets[i+1], 0, 0)
		if len(aggregation) != 0 {
			aggregations = util.MergeMaps(aggregations, aggregation[0]) // result of root node is always a single map, thus [0]
		}
	}
	return aggregations
}

func (cw *ClickhouseQueryTranslator) MakeResponseAggregation(queries []model.QueryWithAggregation, ResultSets [][]model.QueryResultRow) *model.SearchResp {
	var totalCount uint64
	if len(ResultSets) > 0 && len(ResultSets[0]) > 0 && len(ResultSets[0][0].Cols) > 0 {
		// This if: doesn't hurt much, but mostly for tests, never seen need for this on "production".
		totalCount = ResultSets[0][0].Cols[0].Value.(uint64)
	} else {
		logger.WarnWithCtx(cw.Ctx).Msgf("Failed extracting Count value SQL query result [%v]", ResultSets)
		totalCount = 0
	}
	return &model.SearchResp{
		Aggregations: cw.MakeAggregationPartOfResponse(queries, ResultSets),
		Hits: model.SearchHits{
			Hits: []model.SearchHit{}, // seems redundant, but can't remove this, created JSON won't match
			Total: &model.Total{
				Value:    int(totalCount), // TODO just change this to uint64? It works now.
				Relation: "eq",
			},
		},
	}
}

func (cw *ClickhouseQueryTranslator) MakeResponseAggregationMarshalled(queries []model.QueryWithAggregation, ResultSets [][]model.QueryResultRow) ([]byte, error) {
	response := cw.MakeResponseAggregation(queries, ResultSets)
	return response.Marshal()
}

func SearchToAsyncSearchResponse(searchResponse *model.SearchResp, asyncRequestIdStr string, isPartial bool) *model.AsyncSearchEntireResp {
	id := new(string)
	*id = asyncRequestIdStr
	response := model.AsyncSearchEntireResp{
		Response:  *searchResponse,
		ID:        id,
		IsPartial: isPartial,
		IsRunning: isPartial,
	}
	if !isPartial {
		response.CompletionStatus = completionStatusOK
	}
	return &response
}

// GetFieldsList
// TODO flatten tuples, I think (or just don't support them for now, we don't want them at the moment in production schemas)
func (cw *ClickhouseQueryTranslator) GetFieldsList() []string {
	var res []string
	for _, col := range cw.Table.Cols {
		if col.IsFullTextMatch {
			res = append(res, col.Name)
		}
	}
	return res
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
func (cw *ClickhouseQueryTranslator) BuildNRowsQuery(fieldName string, query SimpleQuery, limit int) *model.Query {
	suffixClauses := make([]string, 0)
	if len(query.SortFields) > 0 {
		suffixClauses = append(suffixClauses, "ORDER BY "+strings.Join(query.SortFields, ", "))
	}
	if limit > 0 {
		suffixClauses = append(suffixClauses, "LIMIT "+strconv.Itoa(limit))
	}
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{},
		WhereClause:     query.Sql.Stmt,
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
		cw.AddTokenToHighlight(prefix)
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

func (cw *ClickhouseQueryTranslator) BuildFacetsQuery(fieldName string, query SimpleQuery, limitTodo int) *model.Query {
	suffixClauses := []string{"GROUP BY " + strconv.Quote(fieldName), "ORDER BY count() DESC"}
	innerQuery := model.Query{
		Fields:        []string{fieldName},
		WhereClause:   query.Sql.Stmt,
		SuffixClauses: []string{"LIMIT " + facetsSampleSize},
		FromClause:    cw.Table.FullTableName(),
		CanParse:      true,
	}
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{"count()"},
		SuffixClauses:   suffixClauses,
		FromClause:      "(" + innerQuery.String() + ")",
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
	fieldName := cw.Table.ResolveField(queryMap["field"].(string))
	interval, err := kibana.ParseInterval(cw.extractInterval(queryMap))
	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Msg(err.Error())
	}
	dateTimeType := cw.Table.GetDateTimeType(fieldName)
	if dateTimeType == clickhouse.Invalid {
		logger.ErrorWithCtx(cw.Ctx).Msgf("Invalid date type for field %v", fieldName)
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
