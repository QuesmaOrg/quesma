package queryparser

import (
	"context"
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/kibana"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"mitmproxy/quesma/util"
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
	default:
		logger.WarnWithCtx(cw.Ctx).Msgf("unknown type for highlight token: %T, value: %v", token, token)
	}

}

func (cw *ClickhouseQueryTranslator) ClearTokensToHighlight() {
	cw.tokensToHighlight = []string{}
}

func (cw *ClickhouseQueryTranslator) highlightHit(hit *model.SearchHit, highlighter model.Highlighter, resultRow model.QueryResultRow) {
	for _, col := range resultRow.Cols {
		hit.Fields[col.ColName] = []interface{}{col.Value}
		if highlighter.ShouldHighlight(col.ColName) {
			// check if we have a string here and if so, highlight it
			switch valueAsString := col.Value.(type) {
			case string:
				hit.Highlight[col.ColName] = highlighter.HighlightValue(valueAsString)
			case *string:
				if valueAsString != nil {
					hit.Highlight[col.ColName] = highlighter.HighlightValue(*valueAsString)
				}
			default:
				logger.WarnWithCtx(cw.Ctx).Msgf("unknown type for hit highlighting: %T, value: %v", col.Value, col.Value)
			}
		}
	}

	// TODO: highlight and field checks
	for _, alias := range cw.Table.AliasList() {
		if v, ok := hit.Fields[alias.TargetFieldName]; ok {
			hit.Fields[alias.SourceFieldName] = v
		}
	}
}

func (cw *ClickhouseQueryTranslator) makeSearchResponseNormal(ResultSet []model.QueryResultRow, highlighter model.Highlighter) *model.SearchResp {
	hits := make([]model.SearchHit, len(ResultSet))
	for i, row := range ResultSet {
		hits[i] = model.SearchHit{
			Index:     row.Index,
			Source:    []byte(row.String(cw.Ctx)),
			Fields:    make(map[string][]interface{}),
			Highlight: make(map[string][]string),
		}
		cw.highlightHit(&hits[i], highlighter, ResultSet[i])
	}

	// Set the IDs
	for i, hit := range hits {
		if id, err := cw.computeIdFromDocument(hit); err != nil {
			hits[i].ID = strconv.Itoa(i + 1)
		} else {
			hits[i].ID = id
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
		Shards: model.ResponseShards{
			Total:      1,
			Successful: 1,
			Failed:     0,
		},
	}
}

func emptySearchResponse() model.SearchResp {
	return model.SearchResp{
		Hits: model.SearchHits{
			Hits: []model.SearchHit{},
			Total: &model.Total{
				Value:    0,
				Relation: "eq",
			},
		},
	}

}

func EmptySearchResponse(ctx context.Context) []byte {
	response := emptySearchResponse()
	marshalled, err := response.Marshal()
	if err != nil { // should never ever happen, just in case
		logger.ErrorWithCtx(ctx).Err(err).Msg("failed to marshal empty search response")
	}
	return marshalled
}

func EmptyAsyncSearchResponse(id string, isPartial bool, completionStatus int) ([]byte, error) {
	searchResp := emptySearchResponse()
	asyncSearchResp := SearchToAsyncSearchResponse(&searchResp, id, isPartial, completionStatus)
	return asyncSearchResp.Marshal() // error should never ever happen here
}

func BadRequestParseError(err error) []byte {
	serialized, _ := json.Marshal(ParseErrorResponse{
		Error: Error{
			RootCause: []RootCause{
				{
					Type:   "parsing_exception",
					Reason: err.Error(),
				},
			},
			Type:   "parsing_exception",
			Reason: err.Error(),
		},
		Status: 400,
	},
	)
	return serialized
}

type (
	ParseErrorResponse struct {
		Error  `json:"error"`
		Status int `json:"status"`
	}
	Error struct {
		RootCause []RootCause `json:"root_cause"`
		Type      string      `json:"type"`
		Reason    string      `json:"reason"`
		Line      *int        `json:"line,omitempty"`
		Col       *int        `json:"col,omitempty"`
	}
	RootCause struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
		Line   *int   `json:"line,omitempty"`
		Col    *int   `json:"col,omitempty"`
	}
)

func (cw *ClickhouseQueryTranslator) MakeSearchResponse(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter model.Highlighter) (*model.SearchResp, error) {
	switch typ {
	case model.Normal:
		return cw.makeSearchResponseNormal(ResultSet, highlighter), nil
	case model.Facets, model.FacetsNumeric:
		return cw.makeSearchResponseFacets(ResultSet, typ), nil
	case model.ListByField, model.ListAllFields:
		return cw.makeSearchResponseList(ResultSet, typ, highlighter), nil
	default:
		return nil, fmt.Errorf("unknown SearchQueryType: %v", typ)
	}
}

func (cw *ClickhouseQueryTranslator) MakeSearchResponseMarshalled(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter model.Highlighter) ([]byte, error) {
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

	// Not checking for cast errors here, they may be a lot of them, and error should never happen.
	// One of the better place to allow panic, I think.
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
		default:
			logger.WarnWithCtx(cw.Ctx).Msgf("unknown type for facets doc_count: %T, value: %v",
				ResultSet[0].Cols[model.ResultColDocCountIndex].Value, ResultSet[0].Cols[model.ResultColDocCountIndex].Value)
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
		firstNotNullValueIndex := 0
		for i, row := range ResultSet {
			if row.Cols[model.ResultColKeyIndex].Value != nil {
				firstNotNullValueIndex = i
				break
			}
		}
		if firstNotNullValueIndex == len(ResultSet) {
			aggregations["sample"].(JsonMap)["min_value"] = nil
			aggregations["sample"].(JsonMap)["max_value"] = nil
		} else {
			// Loops below might be a bit slow, as we check types in every iteration.
			// If we see performance issues, we might do separate loop for each type, but it'll be a lot of copy-paste.
			switch ResultSet[firstNotNullValueIndex].Cols[model.ResultColKeyIndex].Value.(type) {
			case int64, uint64, *int64, *uint64, int8, uint8, *int8, *uint8, int16, uint16, *int16, *uint16, int32, uint32, *int32, *uint32:
				firstNotNullValue := util.ExtractInt64(ResultSet[firstNotNullValueIndex].Cols[model.ResultColKeyIndex].Value)
				minValue, maxValue := firstNotNullValue, firstNotNullValue
				for _, row := range ResultSet[firstNotNullValueIndex+1:] {
					if row.Cols[model.ResultColKeyIndex].Value != nil {
						value := util.ExtractInt64(row.Cols[model.ResultColKeyIndex].Value)
						maxValue = max(maxValue, value)
						minValue = min(minValue, value)
					}
				}
				aggregations["sample"].(JsonMap)["min_value"] = JsonMap{"value": minValue}
				aggregations["sample"].(JsonMap)["max_value"] = JsonMap{"value": maxValue}
			case float64, *float64, float32, *float32:
				firstNotNullValue := util.ExtractFloat64(ResultSet[firstNotNullValueIndex].Cols[model.ResultColKeyIndex].Value)
				minValue, maxValue := firstNotNullValue, firstNotNullValue
				for _, row := range ResultSet[firstNotNullValueIndex+1:] {
					if row.Cols[model.ResultColKeyIndex].Value != nil {
						value := util.ExtractFloat64(row.Cols[model.ResultColKeyIndex].Value)
						maxValue = max(maxValue, value)
						minValue = min(minValue, value)
					}
				}
				aggregations["sample"].(JsonMap)["min_value"] = JsonMap{"value": minValue}
				aggregations["sample"].(JsonMap)["max_value"] = JsonMap{"value": maxValue}
			default:
				logger.WarnWithCtx(cw.Ctx).Msgf("unknown type for numeric facet: %T, value: %v",
					ResultSet[0].Cols[model.ResultColKeyIndex].Value, ResultSet[0].Cols[model.ResultColKeyIndex].Value)
				aggregations["sample"].(JsonMap)["min_value"] = JsonMap{"value": nil}
				aggregations["sample"].(JsonMap)["max_value"] = JsonMap{"value": nil}
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
		Shards: model.ResponseShards{
			Total:      1,
			Successful: 1,
			Failed:     0,
		},
	}
}

func (cw *ClickhouseQueryTranslator) computeIdFromDocument(doc model.SearchHit) (string, error) {
	var pseudoUniqueFieldName, pseudoUniqueId string

	if v, err := cw.ClickhouseLM.GetPseudoUniqueField(doc.Index); err != nil {
		return "", fmt.Errorf("missing pseudo unique field for index %s", doc.Index)
	} else {
		pseudoUniqueFieldName = v
	}
	if v, ok := doc.Fields[pseudoUniqueFieldName]; ok {
		if vv, okk := v[0].(time.Time); okk {
			pseudoUniqueId = strconv.Itoa(int(vv.UnixMilli()))
		} else {
			fmt.Sprintf("????? FAILed timestamp type assert : [%v]", v)
		}
	} else {
		logger.Error().Msgf("NO @timestamp FIELD [%v]", v)
		return "", fmt.Errorf("missing @timestamp field")
	}
	//concat := email + fullName
	//logger.Info().Msgf("concat: [%v]", concat)
	//hash := sha1.Sum([]byte(concat))
	//hashEncodedToString := hex.EncodeToString(hash[:])
	//logger.Info().Msgf("hash: [%s]", hashEncodedToString)
	return pseudoUniqueId, nil
}

func (cw *ClickhouseQueryTranslator) makeSearchResponseList(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter model.Highlighter) *model.SearchResp {
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
		cw.highlightHit(&hits[i], highlighter, ResultSet[i])
	}

	// Set the IDs
	for i, hit := range hits {
		if id, err := cw.computeIdFromDocument(hit); err != nil {
			hits[i].ID = strconv.Itoa(i + 1)
		} else {
			hits[i].ID = id
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
		Shards: model.ResponseShards{
			Total:      1,
			Successful: 1,
			Failed:     0,
		},
	}
}

func (cw *ClickhouseQueryTranslator) MakeAsyncSearchResponse(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter model.Highlighter, asyncRequestIdStr string, isPartial bool) (*model.AsyncSearchEntireResp, error) {
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

func (cw *ClickhouseQueryTranslator) MakeAsyncSearchResponseMarshalled(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter model.Highlighter, asyncRequestIdStr string, isPartial bool) ([]byte, error) {
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
		if row1.Cols[i].ExtractValue(cw.Ctx) != row2.Cols[i].ExtractValue(cw.Ctx) {
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

	if query.Aggregators[aggregatorsLevel].Filters {
		subResult["buckets"] = bucketsReturnMap[0]
	} else if query.Aggregators[aggregatorsLevel].Keyed {
		subResult["buckets"] = bucketsReturnMap[0]
	} else if query.Aggregators[aggregatorsLevel].Empty {
		subResult = bucketsReturnMap[0]
	} else {
		subResult["buckets"] = bucketsReturnMap
	}

	desiredLevel := len(query.Aggregators) - 1
	if _, ok := query.Type.(bucket_aggregations.Filters); ok {
		desiredLevel = len(query.Aggregators) - 2
	}
	if aggregatorsLevel == desiredLevel && query.Metadata != nil {
		subResult["meta"] = query.Metadata
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
	cw.postprocessPipelineAggregations(queries, ResultSets)
	for i, query := range queries[aggregation_start_index:] {
		if len(ResultSets) <= i+1 {
			continue
		}
		aggregation := cw.makeResponseAggregationRecursive(query, ResultSets[i+1], 0, 0)
		if len(aggregation) != 0 {
			aggregations = util.MergeMaps(cw.Ctx, aggregations, aggregation[0]) // result of root node is always a single map, thus [0]
		}
	}
	return aggregations
}

func (cw *ClickhouseQueryTranslator) MakeResponseAggregation(queries []model.QueryWithAggregation, ResultSets [][]model.QueryResultRow) *model.SearchResp {
	var totalCount uint64
	if len(ResultSets) > 0 && len(ResultSets[0]) > 0 && len(ResultSets[0][0].Cols) > 0 {
		// This if: doesn't hurt much, but mostly for tests, never seen need for this on "production".
		if val, ok := ResultSets[0][0].Cols[0].Value.(uint64); ok {
			totalCount = val
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("failed extracting Count value SQL query result [%v]. Setting to 0", ResultSets[0])
		}
	} else {
		logger.ErrorWithCtx(cw.Ctx).Msgf("failed extracting Count value SQL query result [%v]. Setting to 0", ResultSets)
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
		Shards: model.ResponseShards{
			Total:      1,
			Successful: 1,
			Failed:     0,
		},
	}
}

func (cw *ClickhouseQueryTranslator) MakeResponseAggregationMarshalled(queries []model.QueryWithAggregation, ResultSets [][]model.QueryResultRow) ([]byte, error) {
	response := cw.MakeResponseAggregation(queries, ResultSets)
	return response.Marshal()
}

func SearchToAsyncSearchResponse(searchResponse *model.SearchResp, asyncRequestIdStr string, isPartial bool, completionStatus int) *model.AsyncSearchEntireResp {
	id := new(string)
	*id = asyncRequestIdStr
	response := model.AsyncSearchEntireResp{
		Response:  *searchResponse,
		ID:        id,
		IsPartial: isPartial,
		IsRunning: isPartial,
	}

	response.CompletionStatus = &completionStatus
	return &response
}

func (cw *ClickhouseQueryTranslator) postprocessPipelineAggregations(queries []model.QueryWithAggregation, ResultSets [][]model.QueryResultRow) {
	queryIterationOrder := cw.sortInTopologicalOrder(queries)
	// fmt.Println("qwerty", queryIterationOrder) let's remove all prints in this function after all pipeline aggregations are merged
	for _, queryIndex := range queryIterationOrder {
		query := queries[queryIndex]
		//fmt.Println(queryIndex, query, ResultSets[queryIndex]) let's remove it after all pipeline aggregations implemented
		pipelineQueryType, isPipeline := query.Type.(model.PipelineQueryType)
		if !isPipeline || !query.HasParentAggregation() {
			continue
		}
		// if we don't send the query, we need process the result ourselves
		parentIndex := -1
		// fmt.Println("queries", queryIndex, "parent:", query.Parent) let's remove it after all pipeline aggregations implemented
		for i, parentQuery := range queries {
			if parentQuery.Name() == query.Parent {
				parentIndex = i
				break
			}
		}
		if parentIndex == -1 {
			logger.WarnWithCtx(cw.Ctx).Msgf("parent index not found for query %v", query)
			continue
		}
		// fmt.Println("ResultSets[i]", ResultSets[queryIndex], queryIndex, parentIndex)
		for _, row := range ResultSets[parentIndex] {
			ResultSets[queryIndex] = append(ResultSets[queryIndex], pipelineQueryType.CalculateResultWhenMissing(row, ResultSets[queryIndex]))
		}
		// fmt.Println("ResultSets[i] - post", ResultSets[queryIndex], "i:", queryIndex, "parent:", parentIndex)
	}
}

func (cw *ClickhouseQueryTranslator) BuildSelectQuery(fields []string, whereClause string) *model.Query {
	return &model.Query{
		Fields:      fields,
		WhereClause: whereClause,
		FromClause:  cw.Table.FullTableName(),
		CanParse:    true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildSimpleSelectQuery(whereClause string, limit int) *model.Query {
	return &model.Query{
		Fields:        []string{"*"},
		WhereClause:   whereClause,
		FromClause:    cw.Table.FullTableName(),
		SuffixClauses: []string{"LIMIT " + strconv.Itoa(cw.applySizeLimit(limit))},
		CanParse:      true,
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

func (cw *ClickhouseQueryTranslator) applySizeLimit(size int) int {
	// FIXME hard limit here to prevent OOM
	const quesmaMaxSize = 10000
	if size > quesmaMaxSize {
		logger.WarnWithCtx(cw.Ctx).Msgf("setting hits size to=%d, got=%d", quesmaMaxSize, size)
		size = quesmaMaxSize
	}
	return size
}

// GetNMostRecentRows fieldName == "*" ==> we query all
// otherwise ==> only this 1 field
func (cw *ClickhouseQueryTranslator) BuildNRowsQuery(fieldName string, query SimpleQuery, limit int) *model.Query {
	suffixClauses := make([]string, 0)
	if len(query.SortFields) > 0 {
		suffixClauses = append(suffixClauses, "ORDER BY "+strings.Join(query.SortFields, ", "))
	}
	if limit > 0 {
		suffixClauses = append(suffixClauses, "LIMIT "+strconv.Itoa(cw.applySizeLimit(limit)))
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
	const defaultDateTimeType = clickhouse.DateTime64
	fieldName := cw.parseFieldField(queryMap, "histogram")
	interval, err := kibana.ParseInterval(cw.extractInterval(queryMap))
	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Msg(err.Error())
	}
	dateTimeType := cw.Table.GetDateTimeType(cw.Ctx, fieldName)
	if dateTimeType == clickhouse.Invalid {
		logger.ErrorWithCtx(cw.Ctx).Msgf("invalid date type for field %v. Using DateTime64 as default.", fieldName)
		dateTimeType = defaultDateTimeType
	}
	return clickhouse.TimestampGroupBy(fieldName, dateTimeType, interval)
}

// sortInTopologicalOrder sorts all our queries to DB, which we send to calculate response for a single query request.
// It sorts them in a way that we can calculate them in the returned order, so any parent aggregation needs to be calculated before its child.
// It's only really needed for pipeline aggregations, as only they have parent-child relationships.
//
// Probably you can create a query with loops in pipeline aggregations, but you can't do it in Kibana from Visualize view,
// so I don't handle it here. We won't panic in such case, only log a warning/error + return non-full results, which is expected,
// as you can't really compute cycled pipeline aggregations.
func (cw *ClickhouseQueryTranslator) sortInTopologicalOrder(queries []model.QueryWithAggregation) []int {
	nameToIndex := make(map[string]int, len(queries))
	for i, query := range queries {
		nameToIndex[query.Name()] = i
	}

	// canSelect[i] == true <=> queries[i] can be selected (it has no parent aggregation, or its parent aggregation is already resolved)
	canSelect := make([]bool, 0, len(queries))
	for _, query := range queries {
		// at the beginning we can select <=> no parent aggregation
		canSelect = append(canSelect, !query.HasParentAggregation())
	}
	alreadySelected := make([]bool, len(queries))
	indexesSorted := make([]int, 0, len(queries))

	// it's a slow O(query_nr^2) algorithm, can be done in O(query_nr), but since query_nr is ~2-10, we don't care
	for len(indexesSorted) < len(queries) {
		lenStart := len(indexesSorted)
		for i, query := range queries {
			if !alreadySelected[i] && canSelect[i] {
				indexesSorted = append(indexesSorted, i)
				alreadySelected[i] = true
				// mark all children as canSelect, as their parent is already resolved (selected)
				for j, maybeChildQuery := range queries {
					if maybeChildQuery.IsChild(query) {
						canSelect[j] = true
					}
				}
			}
		}
		lenEnd := len(indexesSorted)
		if lenEnd == lenStart {
			// without this check, we'd end up in an infinite loop
			logger.WarnWithCtx(cw.Ctx).Msg("could not resolve all parent-child relationships in queries")
			break
		}
	}
	return indexesSorted
}
