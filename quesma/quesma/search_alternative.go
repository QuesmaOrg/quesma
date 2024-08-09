package quesma

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/k0kubun/pp"
	"io"
	"net/http"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/queryparser"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/tracing"
	"time"
)

type executionPlanResult struct {
	isMain       bool
	plan         *model.ExecutionPlan
	err          error
	responseBody []byte
}

type executionPlanExecutor func(ctx context.Context) ([]byte, error)

// runAlternativePlanAndComparison runs the alternative plan and comparison method in the background. It returns a channel to collect the main plan results.
func (q *QueryRunner) runAlternativePlanAndComparison(ctx context.Context, plan *model.ExecutionPlan, alternativePlanExecutor executionPlanExecutor, body types.JSON) chan<- executionPlanResult {

	contextValues := tracing.ExtractValues(ctx)

	numberOfExpectedResults := len([]string{model.MainExecutionPlan, model.AlternativeExecutionPlan})

	optComparePlansCh := make(chan executionPlanResult, numberOfExpectedResults)

	// run alternative plan in the background (generator)
	go func(optComparePlansCh chan<- executionPlanResult) {
		defer recovery.LogPanic()

		// results are passed via channel
		newCtx := tracing.NewContextWithRequest(ctx)
		body, err := alternativePlanExecutor(newCtx)

		optComparePlansCh <- executionPlanResult{
			plan:         plan,
			err:          err,
			responseBody: body,
		}

	}(optComparePlansCh)

	// collector
	go func(optComparePlansCh <-chan executionPlanResult) {
		defer recovery.LogPanic()
		var alternative executionPlanResult
		var main executionPlanResult

		for range numberOfExpectedResults {
			r := <-optComparePlansCh
			logger.InfoWithCtx(ctx).Msgf("received results  %s", r.plan.Name)
			if r.isMain {
				main = r
			} else {
				alternative = r
			}
		}

		bytes, err := body.Bytes()
		if err != nil {
			bytes = []byte("error converting body to bytes")
		}

		toError := func(err error) string {
			if err != nil {
				return err.Error()
			}
			return ""
		}

		abResult := ab_testing.Result{
			Request: ab_testing.Request{
				Path: contextValues.RequestPath,
				Body: string(bytes),
			},

			A: ab_testing.Response{
				Name:  main.plan.Name,
				Body:  string(main.responseBody),
				Time:  time.Since(main.plan.StartTime),
				Error: toError(main.err),
			},

			B: ab_testing.Response{
				Name:  alternative.plan.Name,
				Body:  string(alternative.responseBody),
				Time:  time.Since(alternative.plan.StartTime),
				Error: toError(alternative.err),
			},
			RequestID: contextValues.RequestId,
			OpaqueID:  contextValues.OpaqueId,
		}
		pp.Println("XXXXX Sending A/B Testing Result", abResult)
		q.ABResultsSender.Send(abResult)

	}(optComparePlansCh)

	return optComparePlansCh
}

func (q *QueryRunner) maybeCreateAlternativeExecutionPlan(ctx context.Context, resolvedTableName string, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, body types.JSON, table *clickhouse.Table, isAsync bool) (*model.ExecutionPlan, executionPlanExecutor) {

	// TODO read config here
	//p := q.maybeCreatePancakeExecutionPlan(ctx, resolvedTableName, plan, queryTranslator, body, table, isAsync)
	p, e := q.askElasticAsAnAlternative(ctx, resolvedTableName, plan, queryTranslator, body, table, isAsync)
	return p, e
}

func (t *QueryRunner) askElasticAsAnAlternative(ctx context.Context, resolvedTableName string, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, body types.JSON, table *clickhouse.Table, isAsync bool) (*model.ExecutionPlan, executionPlanExecutor) {

	requestBody, err := body.Bytes()
	if err != nil {
		return nil, nil
	}

	alternativePlan := &model.ExecutionPlan{
		IndexPattern:          plan.IndexPattern,
		QueryRowsTransformers: []model.QueryRowsTransformer{},
		Queries:               []*model.Query{},
		StartTime:             plan.StartTime,
		Name:                  "elastic",
	}

	url := "http://elasticsearch:9200/" + plan.IndexPattern + "/_search"

	return alternativePlan, func(ctx context.Context) ([]byte, error) {

		fmt.Println("XXXXXXXX calling elastic", url)

		if resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody)); err != nil {
			return nil, err
		} else {
			responseBody, err := io.ReadAll(resp.Body)

			if err != nil {
				return nil, err
			}

			if err := resp.Body.Close(); err != nil {
				return nil, err
			}

			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("error calling elastic. got error code: %d", resp.StatusCode)
			}
			return responseBody, nil
		}
	}
}

func (q *QueryRunner) maybeCreatePancakeExecutionPlan(ctx context.Context, resolvedTableName string, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, body types.JSON, table *clickhouse.Table, isAsync bool) executionPlanExecutor {

	props, enabled := q.cfg.IndexConfig[resolvedTableName].GetOptimizerConfiguration(queryparser.PancakeOptimizerName)
	if enabled && props["mode"] == "alternative" {

		hasAggQuery := false
		queriesWithoutAggr := make([]*model.Query, 0)
		for _, query := range plan.Queries {
			switch query.Type.AggregationType() {
			case model.MetricsAggregation, model.BucketAggregation, model.PipelineAggregation:
				hasAggQuery = true
			default:
				queriesWithoutAggr = append(queriesWithoutAggr, query)
			}
		}

		if hasAggQuery {
			if chQueryTranslator, ok := queryTranslator.(*queryparser.ClickhouseQueryTranslator); ok {

				// TODO FIXME check if the original plan has count query
				addCount := false

				if pancakeQueries, err := chQueryTranslator.PancakeParseAggregationJson(body, addCount); err == nil {
					logger.InfoWithCtx(ctx).Msgf("Running alternative pancake queries")
					queries := append(queriesWithoutAggr, pancakeQueries...)
					plan := &model.ExecutionPlan{
						IndexPattern:          plan.IndexPattern,
						QueryRowsTransformers: make([]model.QueryRowsTransformer, len(queries)),
						Queries:               queries,
						StartTime:             plan.StartTime,
						Name:                  "pancake",
					}

					return func(ctx context.Context) ([]byte, error) {

						return q.executeAlternativePlan(ctx, plan, queryTranslator, table, body, false)
					}

				} else {
					// TODO: change to info
					logger.ErrorWithCtx(ctx).Msgf("Error parsing pancake queries: %v", err)
				}
			} else {
				logger.ErrorWithCtx(ctx).Msgf("Alternative plan is not supported for non-clickhouse query translators")
			}
		}
	}
	return nil
}

func (q *QueryRunner) executeAlternativePlan(ctx context.Context, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, table *clickhouse.Table, body types.JSON, isAsync bool) (responseBody []byte, err error) {

	doneCh := make(chan AsyncSearchWithError, 1)

	q.transformQueries(ctx, plan, table)

	if resp, err := q.checkProperties(ctx, plan, table, queryTranslator); err != nil {
		return resp, err
	}

	q.runExecutePlanAsync(ctx, plan, queryTranslator, table, doneCh, nil)

	response := <-doneCh

	if response.err == nil {
		if isAsync {
			asyncResponse := queryparser.SearchToAsyncSearchResponse(response.response, "__quesma_alternative_plan", false, 200)
			responseBody, err = asyncResponse.Marshal()
			if err != nil {
				return nil, err
			}
		} else {
			responseBody, err = response.response.Marshal()
			if err != nil {
				return nil, err
			}
		}
	} else {
		// TODO better error handling
		m := make(map[string]interface{})
		m["error"] = fmt.Sprintf("%v", response.err.Error())
		responseBody, _ = json.MarshalIndent(m, "", "  ")
	}

	bodyAsBytes, _ := body.Bytes()
	contextValues := tracing.ExtractValues(ctx)
	pushAlternativeInfo(q.quesmaManagementConsole, contextValues.RequestId, "", contextValues.OpaqueId, contextValues.RequestPath, bodyAsBytes, response.translatedQueryBody, responseBody, plan.StartTime)

	return responseBody, response.err

}
