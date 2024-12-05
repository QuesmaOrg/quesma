// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"fmt"
	"github.com/goccy/go-json"
	"io"
	"net/http"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/model"
	"quesma/quesma/async_search_storage"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/tracing"
	"quesma/util"
	"quesma_v2/core/mux"
	"time"
)

type executionPlanResult struct {
	isMain       bool
	plan         *model.ExecutionPlan
	err          error
	responseBody []byte
	endTime      time.Time
}

// runABTestingResultsCollector runs the alternative plan and comparison method in the background. It returns a channel to collect the main plan results.
func (q *QueryRunner) runABTestingResultsCollector(ctx context.Context, indexPattern string, body types.JSON) (chan<- executionPlanResult, context.Context) {

	contextValues := tracing.ExtractValues(ctx)

	backgroundContext, cancelFunc := context.WithCancel(tracing.NewContextWithRequest(ctx))

	numberOfExpectedResults := len([]string{model.MainExecutionPlan, model.AlternativeExecutionPlan})

	optComparePlansCh := make(chan executionPlanResult, numberOfExpectedResults)

	// collector
	go func(optComparePlansCh <-chan executionPlanResult) {
		defer recovery.LogPanic()

		var aResult *executionPlanResult
		var bResult *executionPlanResult

		for aResult == nil || bResult == nil {

			select {

			case r := <-optComparePlansCh:
				if r.isMain {
					aResult = &r
				} else {
					bResult = &r
				}

			case <-time.After(1 * time.Minute):
				logger.ErrorWithCtx(ctx).Msgf("timeout waiting for A/B results. A result: %v, B result: %v", aResult, bResult)
				// and cancel the context to stop the execution of the alternative plan
				cancelFunc()
				return
			}
		}

		bytes, err := body.Bytes()
		if err != nil {
			bytes = []byte("error converting body to bytes")
		}

		errorToString := func(err error) string {
			if err != nil {
				return err.Error()
			}
			return ""
		}

		abResult := ab_testing.Result{
			Request: ab_testing.Request{
				Path:      contextValues.RequestPath,
				IndexName: indexPattern,
				Body:      string(bytes),
			},

			A: ab_testing.Response{
				Name:  aResult.plan.Name,
				Body:  string(aResult.responseBody),
				Time:  aResult.endTime.Sub(aResult.plan.StartTime).Seconds(),
				Error: errorToString(aResult.err),
			},

			B: ab_testing.Response{
				Name:  bResult.plan.Name,
				Body:  string(bResult.responseBody),
				Time:  bResult.endTime.Sub(bResult.plan.StartTime).Seconds(),
				Error: errorToString(bResult.err),
			},
			RequestID: contextValues.RequestId,
			OpaqueID:  contextValues.OpaqueId,
		}

		q.ABResultsSender.Send(abResult)

	}(optComparePlansCh)

	return optComparePlansCh, backgroundContext
}

func (q *QueryRunner) executeABTesting(ctx context.Context, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, table *clickhouse.Table, body types.JSON, optAsync *AsyncQuery, decision *mux.Decision, indexPattern string) ([]byte, error) {

	optComparePlansCh, backgroundContext := q.runABTestingResultsCollector(ctx, indexPattern, body)

	var planExecutors []func(ctx context.Context) ([]byte, error)

	for i, connector := range decision.UseConnectors {

		isMainPlan := i == 0 // the first plan is the main plan

		var planExecutor func(ctx context.Context) ([]byte, error)

		switch connector.(type) {

		case *mux.ConnectorDecisionClickhouse:
			planExecutor = func(ctx context.Context) ([]byte, error) {
				plan.Name = config.ClickhouseTarget
				return q.executePlan(ctx, plan, queryTranslator, table, body, optAsync, optComparePlansCh, isMainPlan)
			}

		case *mux.ConnectorDecisionElastic:
			planExecutor = func(ctx context.Context) ([]byte, error) {
				elasticPlan := &model.ExecutionPlan{
					IndexPattern:          plan.IndexPattern,
					QueryRowsTransformers: []model.QueryRowsTransformer{},
					Queries:               []*model.Query{},
					StartTime:             plan.StartTime,
					Name:                  config.ElasticsearchTarget,
				}
				return q.executePlanElastic(ctx, elasticPlan, body, optAsync, optComparePlansCh, isMainPlan)
			}

		default:
			return nil, fmt.Errorf("unknown connector type: %T", connector)
		}
		planExecutors = append(planExecutors, planExecutor)
	}

	if len(planExecutors) != 2 {
		return nil, fmt.Errorf("expected 2 plans (A,B) to execute, but  got %d", len(planExecutors))
	}

	// B plan aka alternative
	go func() {
		defer recovery.LogPanic()
		_, _ = planExecutors[1](backgroundContext) // ignore the result
	}()

	// A plan aka main plan
	// run the first plan in the main thread
	return planExecutors[0](ctx)
}

type asyncElasticSearchWithError struct {
	response            types.JSON
	translatedQueryBody []types.TranslatedSQLQuery
	err                 error
}

func (q *QueryRunner) executePlanElastic(ctx context.Context, plan *model.ExecutionPlan, requestBody types.JSON, optAsync *AsyncQuery, optComparePlansCh chan<- executionPlanResult, abTestingMainPlan bool) (responseBody []byte, err error) {

	contextValues := tracing.ExtractValues(ctx)
	id := contextValues.RequestId
	path := contextValues.RequestPath
	opaqueId := contextValues.OpaqueId

	doneCh := make(chan asyncElasticSearchWithError, 1)

	sendABResult := func(response []byte, err error) {
		optComparePlansCh <- executionPlanResult{
			isMain:       abTestingMainPlan, // TODO
			plan:         plan,
			err:          err,
			responseBody: response,
			endTime:      time.Now(),
		}
	}

	go func() {
		defer recovery.LogAndHandlePanic(ctx, func(err error) {
			doneCh <- asyncElasticSearchWithError{err: err}
		})

		resp, err := q.callElastic(ctx, plan, requestBody, optAsync)

		doneCh <- asyncElasticSearchWithError{response: resp, translatedQueryBody: nil, err: err}
	}()

	if optAsync == nil {
		bodyAsBytes, _ := requestBody.Bytes()
		response := <-doneCh
		if response.err != nil {
			err = response.err
			sendABResult(nil, err)
			return nil, err
		} else {
			responseBody, err = response.response.Bytes()
		}

		pushSecondaryInfo(q.quesmaManagementConsole, id, "", path, bodyAsBytes, response.translatedQueryBody, responseBody, plan.StartTime)
		sendABResult(responseBody, err)
		return responseBody, err
	} else {
		select {

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-time.After(time.Duration(optAsync.waitForResultsMs) * time.Millisecond):
			go func() { // Async search takes longer. Return partial results and wait for
				recovery.LogPanicWithCtx(ctx)
				res := <-doneCh
				responseBody, err = q.storeAsyncSearchWithRaw(q.quesmaManagementConsole, id, optAsync.asyncId, optAsync.startTime, path, requestBody, res.response, res.err, res.translatedQueryBody, true, opaqueId)
				sendABResult(responseBody, err)
			}()
			return q.handlePartialAsyncSearch(ctx, optAsync.asyncId)
		case res := <-doneCh:
			responseBody, err = q.storeAsyncSearchWithRaw(q.quesmaManagementConsole, id, optAsync.asyncId, optAsync.startTime, path, requestBody, res.response, res.err, res.translatedQueryBody, true, opaqueId)
			sendABResult(responseBody, err)
			return responseBody, err
		}
	}
}

func (q *QueryRunner) callElastic(ctx context.Context, plan *model.ExecutionPlan, requestBody types.JSON, optAsync *AsyncQuery) (responseBody types.JSON, err error) {

	url := fmt.Sprintf("%s/_search", plan.IndexPattern)

	client := elasticsearch.NewSimpleClient(&q.cfg.Elasticsearch)

	requestBodyAsBytes, err := requestBody.Bytes()
	if err != nil {
		return nil, err
	}

	resp, err := client.Request(ctx, "POST", url, requestBodyAsBytes)

	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err := resp.Body.Close(); err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		// here we try to parse the response as JSON, if it fails we return the plain text
		responseBody, err = types.ParseJSON(string(data))
		if err != nil {
			responseBody = types.JSON{"plainText": string(data)}
		}

		return responseBody, fmt.Errorf("error calling elastic. got error code: %d", resp.StatusCode)
	}

	contextValues := tracing.ExtractValues(ctx)
	pushPrimaryInfo(q.quesmaManagementConsole, contextValues.RequestId, data, plan.StartTime)

	responseBody, err = types.ParseJSON(string(data))

	if err != nil {
		return nil, err
	}

	return responseBody, nil
}

// this is a copy of  AsyncSearchEntireResp
type AsyncSearchElasticResp struct {
	StartTimeInMillis      uint64 `json:"start_time_in_millis"`
	CompletionTimeInMillis uint64 `json:"completion_time_in_millis"`
	ExpirationTimeInMillis uint64 `json:"expiration_time_in_millis"`
	ID                     string `json:"id,omitempty"`
	IsRunning              bool   `json:"is_running"`
	IsPartial              bool   `json:"is_partial"`
	// CompletionStatus If the async search completed, this field shows the status code of the
	// search.
	// For example, 200 indicates that the async search was successfully completed.
	// 503 indicates that the async search was completed with an error.
	CompletionStatus *int `json:"completion_status,omitempty"`
	Response         any  `json:"response"`
}

func WrapElasticResponseAsAsync(searchResponse any, asyncId string, isPartial bool, completionStatus *int) *AsyncSearchElasticResp {

	response := AsyncSearchElasticResp{
		Response:  searchResponse,
		ID:        asyncId,
		IsPartial: isPartial,
		IsRunning: isPartial,
	}

	response.CompletionStatus = completionStatus
	return &response
}

// TODO rename and change signature to use asyncElasticSearchWithError
func (q *QueryRunner) storeAsyncSearchWithRaw(qmc *ui.QuesmaManagementConsole, id, asyncId string,
	startTime time.Time, path string, body types.JSON, resultJSON types.JSON, resultError error, translatedQueryBody []types.TranslatedSQLQuery, keep bool, opaqueId string) (responseBody []byte, err error) {

	took := time.Since(startTime)

	bodyAsBytes, err := body.Bytes()
	if err != nil {
		return nil, err
	}

	if resultError == nil {
		okStatus := 200
		asyncResponse := WrapElasticResponseAsAsync(resultJSON, asyncId, false, &okStatus)
		responseBody, err = json.MarshalIndent(asyncResponse, "", "  ")
	} else {
		responseBody, err = resultJSON.Bytes()
		if err == nil {
			logger.Warn().Msgf("error while marshalling async search response: %v: ", err)
		}
		err = resultError
	}

	qmc.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
		Id:                     id,
		AsyncId:                asyncId,
		OpaqueId:               opaqueId,
		Path:                   path,
		IncomingQueryBody:      bodyAsBytes,
		QueryBodyTranslated:    translatedQueryBody,
		QueryTranslatedResults: responseBody,
		SecondaryTook:          took,
	})

	if keep {
		compressedBody := responseBody
		isCompressed := false
		if err == nil {
			if compressed, compErr := util.Compress(responseBody); compErr == nil {
				compressedBody = compressed
				isCompressed = true
			}
		}
		q.AsyncRequestStorage.Store(asyncId, async_search_storage.NewAsyncRequestResult(compressedBody, err, time.Now(), isCompressed))
	}

	return
}
