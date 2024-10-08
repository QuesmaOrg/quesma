// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/model"
	"quesma/quesma/config"
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
	endTime      time.Time
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
			isMain:       false,
			plan:         plan,
			err:          err,
			responseBody: body,
			endTime:      time.Now(),
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

		errorToString := func(err error) string {
			if err != nil {
				return err.Error()
			}
			return ""
		}

		abResult := ab_testing.Result{
			Request: ab_testing.Request{
				Path:      contextValues.RequestPath,
				IndexName: plan.IndexPattern,
				Body:      string(bytes),
			},

			A: ab_testing.Response{
				Name:  main.plan.Name,
				Body:  string(main.responseBody),
				Time:  main.endTime.Sub(main.plan.StartTime).Seconds(),
				Error: errorToString(main.err),
			},

			B: ab_testing.Response{
				Name:  alternative.plan.Name,
				Body:  string(alternative.responseBody),
				Time:  alternative.endTime.Sub(alternative.plan.StartTime).Seconds(),
				Error: errorToString(alternative.err),
			},
			RequestID: contextValues.RequestId,
			OpaqueID:  contextValues.OpaqueId,
		}
		q.ABResultsSender.Send(abResult)

	}(optComparePlansCh)

	return optComparePlansCh
}

func (q *QueryRunner) maybeCreateAlternativeExecutionPlan(ctx context.Context, indexes []string, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, body types.JSON, table *clickhouse.Table, isAsync bool) (*model.ExecutionPlan, executionPlanExecutor) {

	// TODO not sure how to check configure when we have multiple indexes
	if len(indexes) != 1 {
		return nil, nil
	}

	resolvedTableName := indexes[0]

	// TODO is should be enabled in a different way. it's not an optimizer
	cfg, disabled := q.cfg.IndexConfig[resolvedTableName].GetOptimizerConfiguration(config.ElasticABOptimizerName)
	if !disabled {
		return q.askElasticAsAnAlternative(ctx, resolvedTableName, plan, queryTranslator, body, table, isAsync, cfg)
	}

	return nil, nil
}

func (q *QueryRunner) askElasticAsAnAlternative(ctx context.Context, resolvedTableName string, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, body types.JSON, table *clickhouse.Table, isAsync bool, props map[string]string) (*model.ExecutionPlan, executionPlanExecutor) {

	// the name of "B" responses
	alternativeName := "elastic"

	// Here we should use backend connector
	//
	elasticUrl := q.cfg.Elasticsearch.Url.String()
	user := q.cfg.Elasticsearch.User
	pass := q.cfg.Elasticsearch.Password

	if url, ok := props["url"]; ok {
		elasticUrl = url
	}

	if u, ok := props["user"]; ok {
		user = u
	}

	if p, ok := props["password"]; ok {
		pass = p
	}

	if name, ok := props["name"]; ok {
		alternativeName = name
	}

	requestBody, err := body.Bytes()
	if err != nil {
		return nil, nil
	}

	alternativePlan := &model.ExecutionPlan{
		IndexPattern:          plan.IndexPattern,
		QueryRowsTransformers: []model.QueryRowsTransformer{},
		Queries:               []*model.Query{},
		StartTime:             plan.StartTime,
		Name:                  alternativeName,
	}

	url := fmt.Sprintf("%s/%s/_search", elasticUrl, plan.IndexPattern)

	return alternativePlan, func(ctx context.Context) ([]byte, error) {

		client := &http.Client{}

		req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))

		if err != nil {
			return nil, err
		}

		elasticsearch.AddBasicAuthIfNeeded(req, user, pass)

		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

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

		contextValues := tracing.ExtractValues(ctx)
		pushPrimaryInfo(q.quesmaManagementConsole, contextValues.RequestId, responseBody, plan.StartTime)

		return responseBody, nil
	}
}
