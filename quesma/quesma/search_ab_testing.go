// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/model"
	"quesma/queryparser"
	"quesma/quesma/async_search_storage"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/table_resolver"
	"quesma/tracing"
	"quesma/util"
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
				logger.InfoWithCtx(ctx).Msgf("received results  %s", r.plan.Name)
				if r.isMain {
					aResult = &r
				} else {
					bResult = &r
				}

			case <-time.After(1 * time.Minute):
				logger.ErrorWithCtx(ctx).Msg("timeout waiting for A/B results")
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

func (q *QueryRunner) executeABTesting(ctx context.Context, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, table *clickhouse.Table, body types.JSON, optAsync *AsyncQuery, decision *table_resolver.Decision, indexPattern string) ([]byte, error) {

	optComparePlansCh, backgroundContext := q.runABTestingResultsCollector(ctx, indexPattern, body)

	var planExecutors []func(ctx context.Context) ([]byte, error)

	for i, connector := range decision.UseConnectors {

		isMainPlan := i == 0

		var planExecutor func(ctx context.Context) ([]byte, error)

		switch connector.(type) {

		case *table_resolver.ConnectorDecisionClickhouse:
			planExecutor = func(ctx context.Context) ([]byte, error) {
				plan.Name = "clickhouse"
				return q.executePlan(ctx, plan, queryTranslator, table, body, optAsync, optComparePlansCh, isMainPlan)
			}

		case *table_resolver.ConnectorDecisionElastic:
			planExecutor = func(ctx context.Context) ([]byte, error) {
				elasticPlan := &model.ExecutionPlan{
					IndexPattern:          plan.IndexPattern,
					QueryRowsTransformers: []model.QueryRowsTransformer{},
					Queries:               []*model.Query{},
					StartTime:             plan.StartTime,
					Name:                  "elastic",
				}
				return q.executePlanElastic(ctx, elasticPlan, body, optAsync, optComparePlansCh, isMainPlan)
			}

		default:
			return nil, fmt.Errorf("unknown connector type: %T", connector)
		}
		planExecutors = append(planExecutors, planExecutor)
	}

	if len(planExecutors) != 2 {
		return nil, fmt.Errorf("expected 2 plans, got %d", len(planExecutors))
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
		fmt.Println("sendABResult", response, err)
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

		resp, err := q.callElastic(ctx, plan, requestBody)
		fmt.Println("callElastic", resp, err)

		doneCh <- asyncElasticSearchWithError{response: resp, translatedQueryBody: nil, err: err}
	}()

	if optAsync == nil {
		bodyAsBytes, _ := requestBody.Bytes()
		response := <-doneCh
		if response.err != nil {
			err = response.err
			return nil, err
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

func (q *QueryRunner) callElastic(ctx context.Context, plan *model.ExecutionPlan, requestBody types.JSON) (responseBody types.JSON, err error) {

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
		return nil, fmt.Errorf("error calling elastic. got error code: %d", resp.StatusCode)
	}

	contextValues := tracing.ExtractValues(ctx)
	pushPrimaryInfo(q.quesmaManagementConsole, contextValues.RequestId, data, plan.StartTime)

	responseBody, err = types.ParseJSON(string(data))

	if err != nil {
		return nil, err
	}

	return responseBody, nil
}

func (q *QueryRunner) storeAsyncSearchWithRaw(qmc *ui.QuesmaManagementConsole, id, asyncId string,
	startTime time.Time, path string, body types.JSON, resultJSON types.JSON, resultError error, translatedQueryBody []types.TranslatedSQLQuery, keep bool, opaqueId string) (responseBody []byte, err error) {

	took := time.Since(startTime)

	bodyAsBytes, err := body.Bytes()
	if err != nil {
		return nil, err
	}

	if resultError == nil {
		responseBody, err = resultJSON.Bytes()
		if err != nil {
			return nil, err
		}
	} else {
		responseBody, _ = queryparser.EmptyAsyncSearchResponse(asyncId, false, 503)
		return responseBody, resultError
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
