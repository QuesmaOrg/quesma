package queryparser

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/kibana"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"mitmproxy/quesma/model/typical_queries"
	"mitmproxy/quesma/queryparser/query_util"
	"mitmproxy/quesma/queryprocessor"
	"mitmproxy/quesma/util"
)

const facetsSampleSize = 20000

type JsonMap = map[string]interface{}

type ClickhouseQueryTranslator struct {
	ClickhouseLM      *clickhouse.LogManager
	Table             *clickhouse.Table
	tokensToHighlight []string
	Ctx               context.Context

	DateMathRenderer string // "clickhouse_interval" or "literal"  if not set, we use "clickhouse_interval"
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

func (cw *ClickhouseQueryTranslator) MakeAsyncSearchResponse(ResultSet []model.QueryResultRow, query *model.Query, asyncRequestIdStr string, isPartial bool) (*model.AsyncSearchEntireResp, error) {
	searchResponse := cw.MakeSearchResponse([]*model.Query{query}, [][]model.QueryResultRow{ResultSet})
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

func (cw *ClickhouseQueryTranslator) finishMakeResponse(query *model.Query, ResultSet []model.QueryResultRow, level int) []model.JsonMap {
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

// DFS algorithm
// 'aggregatorsLevel' - index saying which (sub)aggregation we're handling
// 'selectLevel' - which field from select we're grouping by at current level (or not grouping by, if query.Aggregators[aggregatorsLevel].Empty == true)
func (cw *ClickhouseQueryTranslator) makeResponseAggregationRecursive(query *model.Query,
	ResultSet []model.QueryResultRow, aggregatorsLevel, selectLevel int) []model.JsonMap {

	if len(ResultSet) == 0 {
		// We still should preserve `meta` field if it's there.
		// (we don't preserve it if it's in subaggregations, as we cut them off in case of empty parent aggregation)
		// Both cases tested with Elasticsearch in proxy mode, and our tests.
		metaDict := make(model.JsonMap, 0)
		metaAdded := cw.addMetadataIfNeeded(query, metaDict, aggregatorsLevel)
		if !metaAdded {
			return []model.JsonMap{}
		}
		return []model.JsonMap{{
			query.Aggregators[aggregatorsLevel].Name: metaDict,
		}}
	}

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
	qp := queryprocessor.NewQueryProcessor(cw.Ctx)
	var bucketsReturnMap []model.JsonMap
	if query.Aggregators[aggregatorsLevel].SplitOverHowManyFields == 0 {
		bucketsReturnMap = append(bucketsReturnMap, cw.makeResponseAggregationRecursive(query, ResultSet, aggregatorsLevel+1, selectLevel)...)
	} else {
		// normally it's just 1. It used to be just 1 before multi_terms aggregation, where we usually split over > 1 field
		weSplitOverHowManyFields := query.Aggregators[aggregatorsLevel].SplitOverHowManyFields
		buckets := qp.SplitResultSetIntoBuckets(ResultSet, selectLevel+weSplitOverHowManyFields)
		for _, bucket := range buckets {
			bucketsReturnMap = append(bucketsReturnMap,
				cw.makeResponseAggregationRecursive(query, bucket, aggregatorsLevel+1, selectLevel+weSplitOverHowManyFields)...)
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
	} else if query.Aggregators[aggregatorsLevel].SplitOverHowManyFields == 0 {
		subResult = bucketsReturnMap[0]
	} else {
		subResult["buckets"] = bucketsReturnMap
	}

	_ = cw.addMetadataIfNeeded(query, subResult, aggregatorsLevel)

	result[query.Aggregators[aggregatorsLevel].Name] = subResult
	return []model.JsonMap{result}
}

// addMetadataIfNeeded adds metadata to the `result` dictionary, if needed.
func (cw *ClickhouseQueryTranslator) addMetadataIfNeeded(query *model.Query, result model.JsonMap, aggregatorsLevel int) (added bool) {
	if query.Metadata == nil {
		return false
	}

	desiredLevel := len(query.Aggregators) - 1
	if _, ok := query.Type.(bucket_aggregations.Filters); ok {
		desiredLevel = len(query.Aggregators) - 2
	}
	if aggregatorsLevel == desiredLevel {
		result["meta"] = query.Metadata
		return true
	}
	return false
}

func (cw *ClickhouseQueryTranslator) MakeAggregationPartOfResponse(queries []*model.Query, ResultSets [][]model.QueryResultRow) model.JsonMap {
	aggregations := model.JsonMap{}
	if len(queries) == 0 {
		return aggregations
	}
	cw.postprocessPipelineAggregations(queries, ResultSets)
	for i, query := range queries {
		if i >= len(ResultSets) || query_util.IsNonAggregationQuery(query) {
			continue
		}
		aggregation := cw.makeResponseAggregationRecursive(query, ResultSets[i], 0, 0)
		if len(aggregation) != 0 {
			aggregations = util.MergeMaps(cw.Ctx, aggregations, aggregation[0]) // result of root node is always a single map, thus [0]
		}
	}
	return aggregations
}

func (cw *ClickhouseQueryTranslator) makeHits(queries []*model.Query, results [][]model.QueryResultRow) (queriesWithoutHits []*model.Query, resultsWithoutHits [][]model.QueryResultRow, hit *model.SearchHits) {
	hitsIndex := -1
	for i, query := range queries {
		if query.QueryInfoType == model.ListAllFields || query.QueryInfoType == model.ListByField {
			if hitsIndex != -1 {
				logger.WarnWithCtx(cw.Ctx).Msgf("multiple hits queries found in queries: %v", queries)
			}
			hitsIndex = i
		} else {
			queriesWithoutHits = append(queriesWithoutHits, query)
			resultsWithoutHits = append(resultsWithoutHits, results[i])
		}
	}

	if hitsIndex == -1 {
		return queriesWithoutHits, resultsWithoutHits, nil
	}

	hitsQuery := queries[hitsIndex]
	hitsResultSet := results[hitsIndex]

	if hitsQuery.Type == nil {
		logger.ErrorWithCtx(cw.Ctx).Msgf("hits query type is nil: %v", hitsQuery)
		return queriesWithoutHits, resultsWithoutHits, nil
	}
	hitsPartOfResponse := hitsQuery.Type.TranslateSqlResponseToJson(hitsResultSet, 0)

	hitsResponse := hitsPartOfResponse[0]["hits"].(model.SearchHits)
	return queriesWithoutHits, resultsWithoutHits, &hitsResponse
}

func (cw *ClickhouseQueryTranslator) makeTotalCount(queries []*model.Query, results [][]model.QueryResultRow) (queriesWithoutCount []*model.Query, resultsWithoutCount [][]model.QueryResultRow, total *model.Total) {
	// process count:
	// a) we have count query -> we're done
	// b) we have hits or facets -> we're done
	// c) we don't have above: we return len(biggest resultset(all aggregations))
	totalCount := -1
	relationCount := "eq"
	for i, query := range queries {
		if query.Type != nil {
			if _, isCount := query.Type.(typical_queries.Count); isCount {
				if len(results[i]) > 0 && len(results[i][0].Cols) > 0 {
					if val, ok := results[i][0].Cols[0].Value.(uint64); ok {
						totalCount = int(val)
					} else if val2, ok2 := results[i][0].Cols[0].Value.(int64); ok2 {
						totalCount = int(val2)
					} else {
						logger.ErrorWithCtx(cw.Ctx).Msgf("failed extracting Count value SQL query result [%v]. Setting to 0", results[i])
					}
					if query.SelectCommand.Limit != 0 && totalCount == query.SelectCommand.SampleLimit {
						relationCount = "gte"
					}
				} else {
					logger.ErrorWithCtx(cw.Ctx).Msgf("no results for Count value SQL query result [%v]", results[i])
				}
				continue
			}
		}

		queriesWithoutCount = append(queriesWithoutCount, query)
		resultsWithoutCount = append(resultsWithoutCount, results[i])
	}

	if totalCount != -1 {
		total = &model.Total{
			Value:    totalCount,
			Relation: relationCount,
		}
		return
	}
	for i, query := range queries {
		if query.QueryInfoType == model.Facets || query.QueryInfoType == model.FacetsNumeric {
			totalCount = 0
			for _, row := range results[i] {
				if len(row.Cols) > 0 {
					if val, ok := row.Cols[len(row.Cols)-1].Value.(uint64); ok {
						totalCount += int(val)
					} else if val2, ok2 := row.Cols[len(row.Cols)-1].Value.(int); ok2 {
						totalCount += val2
					} else {
						logger.ErrorWithCtx(cw.Ctx).Msgf("Unknown type of count %v", row.Cols[len(row.Cols)-1].Value)
					}
				}
			}
			total = &model.Total{
				Value:    totalCount,
				Relation: "eq", // likely wrong
			}
			return
		}
	}

	for i, query := range queries {
		if query.QueryInfoType == model.ListAllFields || query.QueryInfoType == model.ListByField {
			totalCount = len(results[i])
			relation := "eq"
			if query.SelectCommand.Limit != 0 && totalCount == query.SelectCommand.Limit {
				relation = "gte"
			}
			total = &model.Total{
				Value:    totalCount,
				Relation: relation,
			}
			return
		}
	}

	// TODO: Look for biggest aggregation

	return
}

func (cw *ClickhouseQueryTranslator) MakeSearchResponse(queries []*model.Query, ResultSets [][]model.QueryResultRow) *model.SearchResp {
	var hits *model.SearchHits
	var total *model.Total
	queries, ResultSets, total = cw.makeTotalCount(queries, ResultSets) // get hits and remove it from queries
	queries, ResultSets, hits = cw.makeHits(queries, ResultSets)        // get hits and remove it from queries

	response := &model.SearchResp{
		Aggregations: cw.MakeAggregationPartOfResponse(queries, ResultSets),
		Shards: model.ResponseShards{
			Total:      1,
			Successful: 1,
			Failed:     0,
		},
	}
	if hits != nil {
		response.Hits = *hits
	} else {
		//response.Hits = model.SearchHits{Hits: make([]model.SearchHit, 0)}
		response.Hits = model.SearchHits{Hits: []model.SearchHit{}}
	}
	if total != nil {
		response.Hits.Total = total
	} else {
		response.Hits.Total = &model.Total{
			Value:    0,
			Relation: "gte",
		}
	}
	return response
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

func (cw *ClickhouseQueryTranslator) postprocessPipelineAggregations(queries []*model.Query, ResultSets [][]model.QueryResultRow) {
	queryIterationOrder := cw.sortInTopologicalOrder(queries)
	// fmt.Println("qwerty", queryIterationOrder) let's remove all prints in this function after all pipeline aggregations are merged
	for _, queryIndex := range queryIterationOrder {
		query := queries[queryIndex]
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
		ResultSets[queryIndex] = pipelineQueryType.CalculateResultWhenMissing(query, ResultSets[parentIndex])
	}
}

func (cw *ClickhouseQueryTranslator) BuildCountQuery(whereClause model.Expr, sampleLimit int) *model.Query {
	return &model.Query{
		SelectCommand: *model.NewSelectCommand(
			[]model.Expr{model.NewCountFunc()},
			nil,
			nil,
			model.NewTableRef(cw.Table.FullTableName()),
			whereClause,
			0,
			sampleLimit,
			false,
		),
		TableName: cw.Table.FullTableName(),
		CanParse:  true,
		Type:      typical_queries.NewCount(cw.Ctx),
	}
}

func (cw *ClickhouseQueryTranslator) BuildNRowsQuery(fieldName string, query *model.SimpleQuery, limit int) *model.Query {
	return query_util.BuildHitsQuery(cw.Ctx, cw.Table.FullTableName(), fieldName, query, limit)
}

func (cw *ClickhouseQueryTranslator) BuildAutocompleteQuery(fieldName string, whereClause model.Expr, limit int) *model.Query {
	return &model.Query{
		SelectCommand: *model.NewSelectCommand(
			[]model.Expr{model.NewColumnRef(fieldName)},
			nil,
			nil,
			model.NewTableRef(cw.Table.FullTableName()),
			whereClause,
			limit,
			0,
			true,
		),
		TableName: cw.Table.FullTableName(),
		CanParse:  true,
	}
}

//lint:ignore U1000 Not used yet
func (cw *ClickhouseQueryTranslator) BuildAutocompleteSuggestionsQuery(fieldName string, prefix string, limit int) *model.Query {
	var whereClause model.Expr
	if len(prefix) > 0 {
		//whereClause = strconv.Quote(fieldName) + " iLIKE '" + prefix + "%'"
		whereClause = model.NewInfixExpr(model.NewColumnRef(fieldName), "iLIKE", model.NewLiteral(prefix+"%"))
		cw.AddTokenToHighlight(prefix)
	}
	return &model.Query{
		SelectCommand: *model.NewSelectCommand(
			[]model.Expr{model.NewColumnRef(fieldName)},
			nil,
			nil,
			model.NewTableRef(cw.Table.FullTableName()),
			whereClause,
			limit,
			0,
			false,
		),
		TableName: cw.Table.FullTableName(),
		CanParse:  true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildFacetsQuery(fieldName string, simpleQuery *model.SimpleQuery, isNumeric bool) *model.Query {
	// FromClause: (SELECT fieldName FROM table WHERE whereClause LIMIT facetsSampleSize)
	var typ model.QueryType
	if isNumeric {
		typ = typical_queries.NewFacetsNumeric(cw.Ctx)
	} else {
		typ = typical_queries.NewFacets(cw.Ctx)
	}

	return &model.Query{
		SelectCommand: *model.NewSelectCommand(
			[]model.Expr{model.NewColumnRef(fieldName), model.NewCountFunc()},
			[]model.Expr{model.NewColumnRef(fieldName)},
			[]model.OrderByExpr{model.NewSortByCountColumn(model.DescOrder)},
			model.NewTableRef(cw.Table.FullTableName()),
			simpleQuery.WhereClause,
			0,
			facetsSampleSize,
			false,
		),
		TableName: cw.Table.FullTableName(),
		CanParse:  true,
		Type:      typ,
	}
}

// earliest == true  <==> we want earliest timestamp
// earliest == false <==> we want latest timestamp
func (cw *ClickhouseQueryTranslator) BuildTimestampQuery(timestampFieldName string, whereClause model.Expr, earliest bool) *model.Query {
	var ordering model.OrderByDirection
	if earliest {
		ordering = model.DescOrder
	} else {
		ordering = model.AscOrder
	}

	return &model.Query{
		SelectCommand: *model.NewSelectCommand(
			[]model.Expr{model.NewColumnRef(timestampFieldName)},
			nil,
			[]model.OrderByExpr{model.NewSortColumn(timestampFieldName, ordering)},
			model.NewTableRef(cw.Table.FullTableName()),
			whereClause,
			1,
			0,
			false,
		),
		TableName: cw.Table.FullTableName(),
		CanParse:  true,
	}
}

func (cw *ClickhouseQueryTranslator) createHistogramPartOfQuery(queryMap QueryMap) model.Expr {
	const defaultDateTimeType = clickhouse.DateTime64
	field := cw.parseFieldField(queryMap, "histogram")
	interval, err := kibana.ParseInterval(cw.extractInterval(queryMap))
	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Msg(err.Error())
	}
	dateTimeType := cw.Table.GetDateTimeTypeFromSelectClause(cw.Ctx, field)
	if dateTimeType == clickhouse.Invalid {
		logger.ErrorWithCtx(cw.Ctx).Msgf("invalid date type for field %+v. Using DateTime64 as default.", field)
		dateTimeType = defaultDateTimeType
	}
	return clickhouse.TimestampGroupBy(field, dateTimeType, interval)
}

// sortInTopologicalOrder sorts all our queries to DB, which we send to calculate response for a single query request.
// It sorts them in a way that we can calculate them in the returned order, so any parent aggregation needs to be calculated before its child.
// It's only really needed for pipeline aggregations, as only they have parent-child relationships.
//
// Probably you can create a query with loops in pipeline aggregations, but you can't do it in Kibana from Visualize view,
// so I don't handle it here. We won't panic in such case, only log a warning/error + return non-full results, which is expected,
// as you can't really compute cycled pipeline aggregations.
func (cw *ClickhouseQueryTranslator) sortInTopologicalOrder(queries []*model.Query) []int {
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
