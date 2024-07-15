// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/typical_queries"
	"quesma/queryparser/query_util"
	"quesma/queryprocessor"
	"quesma/schema"
	"quesma/util"
)

const facetsSampleSize = 20000

type JsonMap = map[string]interface{}

type ClickhouseQueryTranslator struct {
	ClickhouseLM *clickhouse.LogManager
	Table        *clickhouse.Table
	Ctx          context.Context

	DateMathRenderer string // "clickhouse_interval" or "literal"  if not set, we use "clickhouse_interval"
	SchemaRegistry   schema.Registry
}

var completionStatusOK = func() *int { value := 200; return &value }()

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

func (cw *ClickhouseQueryTranslator) MakeAsyncSearchResponse(ResultSet []model.QueryResultRow, query *model.Query, asyncId string, isPartial bool) (*model.AsyncSearchEntireResp, error) {
	searchResponse := cw.MakeSearchResponse([]*model.Query{query}, [][]model.QueryResultRow{ResultSet})
	id := new(string)
	*id = asyncId
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

// DFS algorithm
// 'aggregatorsLevel' - index saying which (sub)aggregation we're handling
// 'selectLevel' - which field from select we're grouping by at current level (or not grouping by, if query.Aggregators[aggregatorsLevel].Empty == true)
func (cw *ClickhouseQueryTranslator) makeResponseAggregationRecursive(query *model.Query,
	ResultSet []model.QueryResultRow, aggregatorsLevel, selectLevel int) model.JsonMap {
	// check if we finish
	if aggregatorsLevel == len(query.Aggregators) {
		return query.Type.TranslateSqlResponseToJson(ResultSet, selectLevel)
	}

	currentAggregator := query.Aggregators[aggregatorsLevel]
	subResult := make(model.JsonMap, 1)

	if len(ResultSet) == 0 {
		// We still should preserve `meta` field if it's there.
		// (we don't preserve it if it's in subaggregations, as we cut them off in case of empty parent aggregation)
		// Both cases tested with Elasticsearch in proxy mode, and our tests.
		if metaAdded := cw.addMetadataIfNeeded(query, subResult, aggregatorsLevel); metaAdded {
			return model.JsonMap{
				currentAggregator.Name: subResult,
			}
		} else {
			return model.JsonMap{}
		}
	}

	// fmt.Println("level1 :/", level1, " level2 B):", level2)
	// or we need to go deeper
	if currentAggregator.SplitOverHowManyFields == 0 {
		subSubResult := cw.makeResponseAggregationRecursive(query, ResultSet, aggregatorsLevel+1, selectLevel)
		// Keyed and Filters aggregations are special and need to be wrapped in "buckets"
		if currentAggregator.Keyed || currentAggregator.Filters {
			subResult["buckets"] = subSubResult
		} else {
			subResult = subSubResult
		}
	} else {
		var bucketsReturnMap []model.JsonMap
		// normally it's just 1. It used to be just 1 before multi_terms aggregation, where we usually split over > 1 field
		qp := queryprocessor.NewQueryProcessor(cw.Ctx)
		weSplitOverHowManyFields := currentAggregator.SplitOverHowManyFields

		// leaf bucket aggregation
		if aggregatorsLevel == len(query.Aggregators)-1 && query.Type.IsBucketAggregation() {
			subResult = cw.makeResponseAggregationRecursive(query, ResultSet, aggregatorsLevel+1, selectLevel+weSplitOverHowManyFields)
			if buckets, exist := subResult["buckets"]; exist {
				for i, bucket := range buckets.([]model.JsonMap) {
					if i < len(ResultSet) {
						bucket[model.KeyAddedByQuesma] = ResultSet[i].Cols[selectLevel].Value
					}
				}
			}
		} else { // need to split here into buckets
			buckets := qp.SplitResultSetIntoBuckets(ResultSet, selectLevel+weSplitOverHowManyFields)
			for _, bucket := range buckets {
				potentialNewBuckets := cw.makeResponseAggregationRecursive(query, bucket, aggregatorsLevel+1, selectLevel+weSplitOverHowManyFields)
				potentialNewBuckets[model.KeyAddedByQuesma] = bucket[0].Cols[selectLevel].Value
				bucketsReturnMap = append(bucketsReturnMap, potentialNewBuckets)
			}
			subResult["buckets"] = bucketsReturnMap
		}
	}

	_ = cw.addMetadataIfNeeded(query, subResult, aggregatorsLevel)

	return model.JsonMap{
		currentAggregator.Name: subResult,
	}
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
			aggregations = util.MergeMaps(cw.Ctx, aggregations, aggregation, model.KeyAddedByQuesma)
		}
	}
	return aggregations
}

func (cw *ClickhouseQueryTranslator) makeHits(queries []*model.Query, results [][]model.QueryResultRow) (queriesWithoutHits []*model.Query, resultsWithoutHits [][]model.QueryResultRow, hit *model.SearchHits) {
	hitsIndex := -1
	for i, query := range queries {
		if _, hasHits := query.Type.(*typical_queries.Hits); hasHits {
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

	hitsResponse := hitsPartOfResponse["hits"].(model.SearchHits)
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
					switch v := results[i][0].Cols[0].Value.(type) {
					case uint64:
						totalCount = int(v)
					case int64:
						totalCount = int(v)
					default:
						logger.ErrorWithCtx(cw.Ctx).Msgf("failed extracting Count value SQL query result [%v]. Setting to 0", results[i])
					}
					// if we have sample limit, we need to check if we hit it. If so, return there could be more results
					if query.SelectCommand.SampleLimit != 0 && totalCount == query.SelectCommand.SampleLimit {
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
		_, isFacetNumeric := query.Type.(*typical_queries.FacetsNumeric)
		_, isFacet := query.Type.(*typical_queries.Facets)
		if isFacetNumeric || isFacet {
			totalCount = 0
			for _, row := range results[i] {
				if len(row.Cols) > 0 {
					switch v := row.Cols[len(row.Cols)-1].Value.(type) {
					case uint64:
						totalCount += int(v)
					case int:
						totalCount += v
					case int64:
						totalCount += int(v)
					default:
						logger.ErrorWithCtx(cw.Ctx).Msgf("Unknown type of count %v %t", v, v)
					}
				}
			}
			// if we have sample limit, we need to check if we hit it. If so, return there could be more results.
			// eq means exact count, gte means greater or equal
			relation := "eq"
			if query.SelectCommand.SampleLimit != 0 && totalCount == query.SelectCommand.SampleLimit {
				relation = "gte"
			}
			total = &model.Total{
				Value:    totalCount,
				Relation: relation,
			}
			return
		}
	}

	for i, query := range queries {
		if _, hasHits := query.Type.(*typical_queries.Hits); hasHits {
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
		response.Hits = model.SearchHits{Hits: []model.SearchHit{}} // empty hits
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

func SearchToAsyncSearchResponse(searchResponse *model.SearchResp, asyncId string, isPartial bool, completionStatus int) *model.AsyncSearchEntireResp {
	id := new(string)
	*id = asyncId
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

func (cw *ClickhouseQueryTranslator) combineQueries(queries []*model.Query) {
	// TODO SET IS_PIPELINE = TRUE FOR PIPELINES!
	for _, query := range queries {
		if !query.NoDBQuery || query.IsPipeline {
			continue
		}

		fmt.Println("NoDBQuery", query)
		parentIndex := -1
		for i, parentCandidate := range queries {
			if len(parentCandidate.Aggregators) == len(query.Aggregators)+1 && parentCandidate.Name() == query.Parent {
				parentIndex = i
				break
			}
		}
		if parentIndex == -1 {
			logger.WarnWithCtx(cw.Ctx).Msgf("parent index not found for query %v", query)
			continue
		}
		fmt.Println("parentIdx:", parentIndex)
		parentQuery := queries[parentIndex]
		parentQuery.SelectCommand.OrderBy = append(parentQuery.SelectCommand.OrderBy,
			model.OrderByExpr{Exprs: parentQuery.SelectCommand.Columns[len(parentQuery.SelectCommand.Columns)-1:], Direction: model.DescOrder})
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
			[]model.Expr{},
			0,
			sampleLimit,
			false,
			nil,
		),
		TableName: cw.Table.FullTableName(),
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
			[]model.Expr{},
			limit,
			0,
			true,
			nil,
		),
		TableName: cw.Table.FullTableName(),
	}
}

//lint:ignore U1000 Not used yet
func (cw *ClickhouseQueryTranslator) BuildAutocompleteSuggestionsQuery(fieldName string, prefix string, limit int) *model.Query {
	var whereClause model.Expr
	if len(prefix) > 0 {
		//whereClause = strconv.Quote(fieldName) + " iLIKE '" + prefix + "%'"
		whereClause = model.NewInfixExpr(model.NewColumnRef(fieldName), "iLIKE", model.NewLiteral(prefix+"%"))
	}
	return &model.Query{
		SelectCommand: *model.NewSelectCommand(
			[]model.Expr{model.NewColumnRef(fieldName)},
			nil,
			nil,
			model.NewTableRef(cw.Table.FullTableName()),
			whereClause,
			[]model.Expr{},
			limit,
			0,
			false,
			nil,
		),
		TableName: cw.Table.FullTableName(),
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
			[]model.Expr{},
			0,
			facetsSampleSize,
			false,
			nil,
		),
		TableName: cw.Table.FullTableName(),
		Type:      typ,
	}
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
