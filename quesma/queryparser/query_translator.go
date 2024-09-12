// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/model/typical_queries"
	"quesma/queryparser/query_util"
	"quesma/quesma/config"
	"quesma/schema"
	"quesma/util"
	"strings"
)

const facetsSampleSize = 20000

type JsonMap = map[string]interface{}

type ClickhouseQueryTranslator struct {
	ClickhouseLM *clickhouse.LogManager
	Table        *clickhouse.Table // TODO this will be removed
	Ctx          context.Context

	DateMathRenderer string // "clickhouse_interval" or "literal"  if not set, we use "clickhouse_interval"

	SchemaRegistry    schema.Registry
	IncomingIndexName string
	Config            *config.QuesmaConfiguration
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

func (cw *ClickhouseQueryTranslator) MakeAggregationPartOfResponse(queries []*model.Query, ResultSets [][]model.QueryResultRow) (model.JsonMap, error) {
	aggregations := model.JsonMap{}
	if len(queries) == 0 {
		return aggregations, nil
	}
	cw.postprocessPipelineAggregations(queries, ResultSets)

	for i, query := range queries {
		if pancake, isPancake := query.Type.(PancakeQueryType); isPancake {
			if i >= len(ResultSets) {
				continue
			}
			aggregation, err := pancake.RenderAggregationJson(cw.Ctx, ResultSets[i])
			if err != nil {
				return nil, err
			}

			aggregations = util.MergeMaps(cw.Ctx, aggregations, aggregation, "key")
		}
	}
	return aggregations, nil
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
	hitsPartOfResponse := hitsQuery.Type.TranslateSqlResponseToJson(hitsResultSet)

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

	for queryIdx, query := range queries {
		if pancake, isPancake := query.Type.(PancakeQueryType); isPancake {
			totalCountAgg := pancake.ReturnTotalCount()
			if totalCountAgg != nil {
				if len(results[queryIdx]) == 0 {
					continue
				}
				totalCount = 0
				for rowIdx, row := range results[queryIdx] {
					// sum over all rows
					if len(row.Cols) == 0 {
						continue
					}

					// if group by key exists + it's the same as last, we have already counted it and need to continue
					newKey := true
					if rowIdx != 0 { // for first row we always have a new key
						for colIdx, cell := range row.Cols {
							// find first group by key
							if strings.HasSuffix(cell.ColName, "__key_0") {
								if row.Cols[colIdx].Value == results[queryIdx][rowIdx-1].Cols[colIdx].Value {
									newKey = false
								}
								break
							}
						}
					}
					if !newKey {
						continue
					}

					// find the count column
					for _, cell := range row.Cols {
						// FIXME THIS is hardcoded for now, as we don't have a way to get the name of the column
						if cell.ColName == "metric____quesma_total_count_col_0" {
							switch v := cell.Value.(type) {
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
				}
				total = &model.Total{
					Value:    totalCount,
					Relation: "eq",
				}
				return
			}
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

	aggregations, err := cw.MakeAggregationPartOfResponse(queries, ResultSets)

	response := &model.SearchResp{
		Aggregations: aggregations,
		Timeout:      err != nil, // if there was an error, we should return that results are partial
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
		ResultSets[queryIndex] = pipelineQueryType.CalculateResultWhenMissing(ResultSets[parentIndex])
	}
}

func (cw *ClickhouseQueryTranslator) BuildCountQuery(whereClause model.Expr, sampleLimit int) *model.Query {
	return &model.Query{
		SelectCommand: *model.NewSelectCommand(
			[]model.Expr{model.NewCountFunc()},
			nil,
			nil,
			model.NewTableRef(model.SingleTableNamePlaceHolder),
			whereClause,
			[]model.Expr{},
			0,
			sampleLimit,
			false,
			nil,
			nil,
		),
		Type: typical_queries.NewCount(cw.Ctx),
	}
}

func (cw *ClickhouseQueryTranslator) BuildNRowsQuery(fieldNames []string, query *model.SimpleQuery, limit int) *model.Query {
	return query_util.BuildHitsQuery(cw.Ctx, model.SingleTableNamePlaceHolder, fieldNames, query, limit)
}

func (cw *ClickhouseQueryTranslator) BuildAutocompleteQuery(fieldName, tableName string, whereClause model.Expr, limit int) *model.Query {
	return &model.Query{
		SelectCommand: *model.NewSelectCommand(
			[]model.Expr{model.NewColumnRef(fieldName)},
			nil,
			nil,
			model.NewTableRef(tableName),
			whereClause,
			[]model.Expr{},
			limit,
			0,
			true,
			nil,
			nil,
		),
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
