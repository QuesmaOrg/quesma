package quesma

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"mitmproxy/quesma/util"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const asyncQueriesLimit = 1000

var asyncRequestId atomic.Int64

type AsyncRequestResult struct {
	responseBody []byte
	added        time.Time
	isCompressed bool
	err          error
}

type AsyncQueryContext struct {
	id     string
	ctx    context.Context
	cancel context.CancelFunc
	added  time.Time
}

type QueryRunner struct {
	executionCtx         context.Context
	cancel               context.CancelFunc
	AsyncRequestStorage  *concurrent.Map[string, AsyncRequestResult]
	AsyncQueriesContexts *concurrent.Map[string, *AsyncQueryContext]
}

func NewQueryRunner() *QueryRunner {
	ctx, cancel := context.WithCancel(context.Background())
	return &QueryRunner{executionCtx: ctx, cancel: cancel, AsyncRequestStorage: concurrent.NewMap[string, AsyncRequestResult](), AsyncQueriesContexts: concurrent.NewMap[string, *AsyncQueryContext]()}
}

func NewAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, id string) *AsyncQueryContext {
	return &AsyncQueryContext{ctx: ctx, cancel: cancel, added: time.Now(), id: id}
}

func (q *QueryRunner) handleCount(ctx context.Context, indexPattern string, lm *clickhouse.LogManager) (int64, error) {
	indexes := lm.ResolveIndexes(indexPattern)
	if len(indexes) == 0 {
		if elasticsearch.IsIndexPattern(indexPattern) {
			return 0, nil
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not resolve table name for [%s]", indexPattern)
			return -1, errors.New("could not resolve table name")
		}
	}

	if len(indexes) == 1 {
		return lm.Count(ctx, indexes[0])
	} else {
		return lm.CountMultiple(ctx, indexes...)
	}
}

func (q *QueryRunner) handleSearch(ctx context.Context, indexPattern string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole) ([]byte, error) {
	return q.handleSearchCommon(ctx, indexPattern, body, lm, quesmaManagementConsole, false, 0, false)
}

func (q *QueryRunner) handleAsyncSearch(ctx context.Context, indexPattern string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole, waitForResultsMs int, keepOnCompletion bool) ([]byte, error) {
	return q.handleSearchCommon(ctx, indexPattern, body, lm, quesmaManagementConsole, true, waitForResultsMs, keepOnCompletion)
}

func (q *QueryRunner) handleSearchCommon(ctx context.Context, indexPattern string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole, async bool, waitForResultsMs int, keepOnCompletion bool) ([]byte, error) {

	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	resolved := lm.ResolveIndexes(indexPattern)
	if len(resolved) == 0 {
		if elasticsearch.IsIndexPattern(indexPattern) {
			if async {
				return queryparser.EmptyAsyncSearchResponse(id), nil
			} else {
				return queryparser.EmptySearchResponse(), nil
			}
		} else {
			logger.WarnWithCtx(ctx).Str(logger.RID, id).Msgf("could not resolve any table name for [%s]", indexPattern)
			return nil, errors.New("could not resolve table name")
		}
	} else if len(resolved) > 1 { // async search never worked for multiple indexes, TODO fix
		logger.WarnWithCtx(ctx).Str(logger.RID, id).Msgf("could not resolve multiple table names for [%s]", indexPattern)
		resolved = resolved[1:2]
	}

	var responseBody, translatedQueryBody []byte

	startTime := time.Now()
	pushSecondaryInfoToManagementConsole := func() {
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     id,
			IncomingQueryBody:      body,
			QueryBodyTranslated:    translatedQueryBody,
			QueryRawResults:        []byte{},
			QueryTranslatedResults: responseBody,
			SecondaryTook:          time.Since(startTime),
		})
	}

	asyncRequestIdStr := generateAsyncRequestId()
	doneCh := make(chan struct{}, 1)

	var hits, hitsFallback []model.QueryResultRow
	var aggregationResults [][]model.QueryResultRow
	oldHandlingUsed := false
	newAggregationHandlingUsed := false
	hitsPresent := false

	tables := lm.GetTableDefinitions()

	// TODO: variables below should be per table. Now they are not, as we only support one table.
	var queryTranslator *queryparser.ClickhouseQueryTranslator
	var highlighter queryparser.Highlighter
	var aggregations []model.QueryWithAggregation
	var err error
	var queryInfo model.SearchQueryInfo
	var count int

	for _, resolvedTableName := range resolved {
		table, _ := tables.Load(resolvedTableName)
		queryTranslator = &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: ctx}
		var simpleQuery queryparser.SimpleQuery
		simpleQuery, queryInfo, highlighter = queryTranslator.ParseQuery(string(body))
		if simpleQuery.CanParse {
			if ((queryInfo.Typ == model.ListByField || queryInfo.Typ == model.ListAllFields || queryInfo.Typ == model.Normal) && !bytes.Contains(body, []byte("aggs"))) || queryInfo.Typ == model.Facets || queryInfo.Typ == model.FacetsNumeric {
				logger.InfoWithCtx(ctx).Msgf("Received search request, type: %v, async: %v", queryInfo.Typ, async)

				var allReferencedFields = make([]string, 0)
				allReferencedFields = append(allReferencedFields, queryInfo.RequestedFields...)
				for _, field := range simpleQuery.SortFields {
					allReferencedFields = append(allReferencedFields, strings.ReplaceAll(strings.Fields(field)[0], `"`, ""))
				}

				for _, property := range allReferencedFields {
					if property != "*" && !table.HasColumn(property) {
						logger.DebugWithCtx(ctx).Msgf("Property %s not found in table %s", property, table.Name)
						if elasticsearch.IsIndexPattern(indexPattern) {
							return queryparser.EmptySearchResponse(), nil
						} else {
							return nil, fmt.Errorf("property %s not found in table %s", property, table.Name)
						}
					}
				}

				oldHandlingUsed = true
				if async {
					go q.searchWorker(ctx, quesmaManagementConsole, asyncRequestIdStr, queryTranslator, table, body, doneCh, async)
				} else {
					translatedQueryBody, hits = q.searchWorker(ctx, quesmaManagementConsole, asyncRequestIdStr, queryTranslator, table, body, doneCh, async)
				}
			} else if aggregations, err = queryTranslator.ParseAggregationJson(string(body)); err == nil {
				newAggregationHandlingUsed = true
				if async {
					go q.searchAggregationWorker(ctx, quesmaManagementConsole, asyncRequestIdStr, aggregations, queryTranslator, table, body, doneCh, async)
				} else {
					translatedQueryBody, aggregationResults = q.searchAggregationWorker(ctx, quesmaManagementConsole, asyncRequestIdStr, aggregations, queryTranslator, table, body, doneCh, async)
				}
			}

			if !async && queryInfo.Size > 0 {
				hitsPresent = true
				var fieldName string
				if queryInfo.Typ == model.ListByField {
					fieldName = queryInfo.FieldName
				} else {
					fieldName = "*"
				}
				listQuery := queryTranslator.BuildNRowsQuery(fieldName, simpleQuery, queryInfo.Size)
				hitsFallback, err = queryTranslator.ClickhouseLM.ProcessNRowsQuery(ctx, table, listQuery)
				if err != nil {
					logger.ErrorWithCtx(ctx).Msgf("Error processing fallback query: %v", err)
					pushSecondaryInfoToManagementConsole()
					return responseBody, err
				}
				countQuery := queryTranslator.BuildSimpleCountQuery(simpleQuery.Sql.Stmt)
				countResult, err := queryTranslator.ClickhouseLM.ProcessSimpleSelectQuery(ctx, table, countQuery)
				if err != nil {
					logger.ErrorWithCtx(ctx).Msgf("Error processing count query: %v", err)
					pushSecondaryInfoToManagementConsole()
					return responseBody, err
				}
				if len(countResult) > 0 {
					// This if only for tests... On production it'll never be 0.
					// When e.g. sqlmock starts supporting uint64, we can remove it.
					count = int(countResult[0].Cols[0].Value.(uint64))
				}
			}
		} else {
			responseBody = []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
			logger.ErrorWithCtxAndReason(ctx, "Quesma generated invalid SQL query").Msg(string(responseBody))
			pushSecondaryInfoToManagementConsole()
			return responseBody, errors.New(string(responseBody))
		}
	}

	/* TODO add this somehow, somewhere
		if err != nil {
		if elasticsearch.IsIndexPattern(indexPattern) {
			logger.WarnWithCtx(ctx).Msgf("Unprocessable: %s, err: %s, resolving to empty (desired behaviour)", fullQuery.String(), err.Error())
			continue
		} else {
			errorMsg := fmt.Sprintf("Error processing query: %s, err: %s", fullQuery.String(), err.Error())
			logger.ErrorWithCtx(ctx).Msg(errorMsg)
			responseBody = []byte(errorMsg)
			pushSecondaryInfoToManagementConsole()
			return responseBody, err
		}
	}
	*/

	if !async {
		var response, responseHits *model.SearchResp = nil, nil
		err = nil
		if oldHandlingUsed {
			response, err = queryTranslator.MakeSearchResponse(hits, queryInfo.Typ, highlighter)
		} else if newAggregationHandlingUsed {
			response = queryTranslator.MakeResponseAggregation(aggregations, aggregationResults)
		}
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("Error making response: %v rows: %v", err, hits)
			pushSecondaryInfoToManagementConsole()
			return responseBody, err
		}

		if hitsPresent {
			if response == nil {
				response, err = queryTranslator.MakeSearchResponse(hitsFallback, queryInfo.Typ, highlighter)
			} else {
				responseHits, err = queryTranslator.MakeSearchResponse(hitsFallback, queryInfo.Typ, highlighter)
				response.Hits = responseHits.Hits
			}
			response.Hits.Total.Value = count
		}
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("Error making response: %v rows: %v", err, hitsFallback)
		}
		responseBody, err = response.Marshal()

		pushSecondaryInfoToManagementConsole()
		return responseBody, err
	} else {
		if waitForResultsMs == 0 {
			return createEmptyAsyncSearchResponse(asyncRequestIdStr, true, 200)
		}
		select {
		case <-time.After(time.Duration(waitForResultsMs) * time.Millisecond):
			return q.handlePartialAsyncSearch(asyncRequestIdStr, quesmaManagementConsole)
		case <-doneCh:
			res, err := q.handlePartialAsyncSearch(asyncRequestIdStr, quesmaManagementConsole)
			if !keepOnCompletion {
				q.AsyncRequestStorage.Delete(asyncRequestIdStr)
			}
			return res, err
		}
	}
}

func generateAsyncRequestId() string {
	return "quesma_async_search_id_" + strconv.FormatInt(asyncRequestId.Add(1), 10)
}

func createEmptyAsyncSearchResponse(id string, isPartial bool, status int) ([]byte, error) {
	hits := make([]model.SearchHit, 0) // need to remove count result from hits
	total := &model.Total{
		Value: 0,
	}
	response := model.AsyncSearchEntireResp{
		Response: model.SearchResp{
			Hits: model.SearchHits{
				Total: total,
				Hits:  hits,
			},
		},
	}
	response.ID = &id
	response.IsPartial = isPartial
	response.IsRunning = isPartial
	response.CompletionStatus = &status
	return json.Marshal(response)
}

func (q *QueryRunner) handlePartialAsyncSearch(id string, quesmaManagementConsole *ui.QuesmaManagementConsole) ([]byte, error) {
	if !strings.Contains(id, "quesma_async_search_id_") {
		return createEmptyAsyncSearchResponse(id, false, 503)
	}
	if result, ok := q.AsyncRequestStorage.Load(id); ok {
		if result.err != nil {
			q.AsyncRequestStorage.Delete(id)
			return createEmptyAsyncSearchResponse(id, false, 503)
		}
		q.AsyncRequestStorage.Delete(id)
		// We use zstd to conserve memory, as we have a lot of async queries
		if result.isCompressed {
			return util.Decompress(result.responseBody)
		}
		return result.responseBody, nil
	} else {
		const isPartial = true
		return createEmptyAsyncSearchResponse(id, isPartial, 200)
	}
}

func (q *QueryRunner) deleteAsyncSeach(id string) ([]byte, error) {
	if !strings.Contains(id, "quesma_async_search_id_") {
		return nil, errors.New("invalid quesma async search id : " + id)
	}
	q.AsyncRequestStorage.Delete(id)
	return []byte{}, nil
}

func (q *QueryRunner) reachedQueriesLimit(asyncRequestIdStr string, doneCh chan struct{}) bool {
	if q.AsyncRequestStorage.Size() < asyncQueriesLimit {
		return false
	}
	q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{err: errors.New("too many async queries"), added: time.Now(), isCompressed: false})
	logger.Error().Msgf("Cannot handle %s, too many async queries", asyncRequestIdStr)
	doneCh <- struct{}{}
	return true
}

func (q *QueryRunner) addAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, asyncRequestIdStr string) {
	q.AsyncQueriesContexts.Store(asyncRequestIdStr, NewAsyncQueryContext(ctx, cancel, asyncRequestIdStr))
}

func (q *QueryRunner) searchWorkerCommon(ctx context.Context, quesmaManagementConsole *ui.QuesmaManagementConsole, asyncRequestIdStr string, queryTranslator *queryparser.ClickhouseQueryTranslator,
	table *clickhouse.Table, body []byte, doneCh chan struct{}, async bool) (translatedQueryBody []byte, hits []model.QueryResultRow) {
	if async && q.reachedQueriesLimit(asyncRequestIdStr, doneCh) {
		return
	}

	var err error
	var fullQuery *model.Query
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	startTime := time.Now()
	simpleQuery, queryInfo, highlighter := queryTranslator.ParseQuery(string(body))
	var dbQueryCtx context.Context
	if async {
		var dbCancel context.CancelFunc
		dbQueryCtx, dbCancel = context.WithCancel(context.Background())
		q.addAsyncQueryContext(dbQueryCtx, dbCancel, asyncRequestIdStr)
	} else {
		dbQueryCtx = ctx
	}

	switch queryInfo.Typ {
	case model.CountAsync:
		fullQuery = queryTranslator.BuildSimpleCountQuery(simpleQuery.Sql.Stmt)
		hits, err = queryTranslator.ClickhouseLM.ProcessSimpleSelectQuery(dbQueryCtx, table, fullQuery)

	case model.Facets, model.FacetsNumeric:
		// queryInfo = (Facets, fieldName, Limit results, Limit last rows to look into)
		fullQuery = queryTranslator.BuildFacetsQuery(queryInfo.FieldName, simpleQuery, queryInfo.I2)
		hits, err = queryTranslator.ClickhouseLM.ProcessFacetsQuery(dbQueryCtx, table, fullQuery)

	case model.ListByField:
		// queryInfo = (ListByField, fieldName, 0, LIMIT)
		fullQuery = queryTranslator.BuildNRowsQuery(queryInfo.FieldName, simpleQuery, queryInfo.I2)
		hits, err = queryTranslator.ClickhouseLM.ProcessNRowsQuery(dbQueryCtx, table, fullQuery)

	case model.ListAllFields:
		// queryInfo = (ListAllFields, "*", 0, LIMIT)
		fullQuery = queryTranslator.BuildNRowsQuery("*", simpleQuery, queryInfo.I2)
		hits, err = queryTranslator.ClickhouseLM.ProcessNRowsQuery(dbQueryCtx, table, fullQuery)

	case model.Normal:
		fullQuery = queryTranslator.BuildSimpleSelectQuery(simpleQuery.Sql.Stmt)
		hits, err = queryTranslator.ClickhouseLM.ProcessSimpleSelectQuery(dbQueryCtx, table, fullQuery)

	default:
		panic(fmt.Sprintf("Unknown query type: %v", queryInfo.Typ))
	}
	if fullQuery != nil {
		translatedQueryBody = []byte(fullQuery.String())
	}
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Rows: %+v, err: %+v", hits, err)
		if async {
			if err != nil {
				q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{responseBody: []byte{}, added: time.Now(), err: err, isCompressed: false})
				doneCh <- struct{}{}
				return
			}
		}
	}
	if async {
		searchResponse, err := queryTranslator.MakeSearchResponse(hits, queryInfo.Typ, highlighter)
		if err != nil {
			logger.Error().Msgf("Error making response: %v rows: %v", err, hits)
			q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{responseBody: []byte{}, added: time.Now(), err: err, isCompressed: false})
			doneCh <- struct{}{}
			return
		}
		const isPartial = false
		asyncSearchResponse := queryparser.SearchToAsyncSearchResponse(searchResponse, id, isPartial)
		responseBody, err := asyncSearchResponse.Marshal()
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     id,
			IncomingQueryBody:      body,
			QueryBodyTranslated:    translatedQueryBody,
			QueryRawResults:        []byte{},
			QueryTranslatedResults: responseBody,
			SecondaryTook:          time.Since(startTime),
		})
		isCompressed := false

		if err == nil {
			if compressed, compErr := util.Compress(responseBody); compErr == nil {
				responseBody = compressed
				isCompressed = true
			}
		}
		q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{responseBody: responseBody, added: time.Now(), err: err, isCompressed: isCompressed})
		doneCh <- struct{}{}
	}
	return
}

func (q *QueryRunner) searchWorker(ctx context.Context, quesmaManagementConsole *ui.QuesmaManagementConsole, asyncRequestIdStr string, queryTranslator *queryparser.ClickhouseQueryTranslator,
	table *clickhouse.Table, body []byte, doneCh chan struct{}, async bool) (translatedQueryBody []byte, hits []model.QueryResultRow) {
	if !async {
		return q.searchWorkerCommon(ctx, quesmaManagementConsole, asyncRequestIdStr, queryTranslator, table, body, doneCh, async)
	} else {
		select {
		case <-q.executionCtx.Done():
			return
		default:
			_, _ = q.searchWorkerCommon(ctx, quesmaManagementConsole, asyncRequestIdStr, queryTranslator, table, body, doneCh, async)
			return
		}
	}
}

func (q *QueryRunner) searchAggregationWorkerCommon(ctx context.Context, quesmaManagementConsole *ui.QuesmaManagementConsole, asyncRequestIdStr string, aggregations []model.QueryWithAggregation,
	queryTranslator *queryparser.ClickhouseQueryTranslator, table *clickhouse.Table, body []byte,
	doneCh chan struct{}, async bool) (translatedQueryBody []byte, resultRows [][]model.QueryResultRow) {

	if async && q.reachedQueriesLimit(asyncRequestIdStr, doneCh) {
		return
	}

	sqls := ""
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	startTime := time.Now()
	var dbQueryCtx context.Context
	if async {
		var dbCancel context.CancelFunc
		dbQueryCtx, dbCancel = context.WithCancel(context.Background())
		q.addAsyncQueryContext(dbQueryCtx, dbCancel, asyncRequestIdStr)
	} else {
		dbQueryCtx = ctx
	}
	logger.InfoWithCtx(ctx).Msg("We're using new Aggregation handling.")
	for _, agg := range aggregations {
		logger.InfoWithCtx(ctx).Msg(agg.String()) // I'd keep for now until aggregations work fully
		rows, err := queryTranslator.ClickhouseLM.ProcessGeneralAggregationQuery(dbQueryCtx, table, &agg.Query)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msg(err.Error())
			continue
		}
		resultRows = append(resultRows, rows)
		sqls += agg.Query.String() + "\n"
	}
	translatedQueryBody = []byte(sqls)
	if async {
		const isPartial = false
		searchResponse := queryTranslator.MakeResponseAggregation(aggregations, resultRows)
		asyncSearchResponse := queryparser.SearchToAsyncSearchResponse(searchResponse, id, isPartial)
		responseBody, err := asyncSearchResponse.Marshal()
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     id,
			IncomingQueryBody:      body,
			QueryBodyTranslated:    translatedQueryBody,
			QueryRawResults:        []byte{},
			QueryTranslatedResults: responseBody,
			SecondaryTook:          time.Since(startTime),
		})
		isCompressed := false
		if err == nil {
			if compressed, compErr := util.Compress(responseBody); compErr == nil {
				responseBody = compressed
				isCompressed = true
			}
		}
		q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{responseBody: responseBody, added: time.Now(),
			isCompressed: isCompressed, err: err})
		doneCh <- struct{}{}
	}
	return
}

func (q *QueryRunner) searchAggregationWorker(ctx context.Context, quesmaManagementConsole *ui.QuesmaManagementConsole, asyncRequestIdStr string, aggregations []model.QueryWithAggregation,
	queryTranslator *queryparser.ClickhouseQueryTranslator, table *clickhouse.Table, body []byte,
	doneCh chan struct{}, async bool) (translatedQueryBody []byte, resultRows [][]model.QueryResultRow) {
	if !async {
		return q.searchAggregationWorkerCommon(ctx, quesmaManagementConsole, asyncRequestIdStr, aggregations, queryTranslator, table, body, doneCh, async)
	} else {
		select {
		case <-q.executionCtx.Done():
			return
		default:
			_, _ = q.searchAggregationWorkerCommon(ctx, quesmaManagementConsole, asyncRequestIdStr, aggregations, queryTranslator, table, body, doneCh, async)
			return
		}
	}
}

func (q *QueryRunner) Close() {
	q.cancel()
	logger.Info().Msg("QueryRunner Stopped")
}
