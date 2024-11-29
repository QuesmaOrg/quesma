// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/feature"
	"quesma/ingest"
	"quesma/logger"
	"quesma/proxy"
	"quesma/queryparser"
	"quesma/quesma/config"
	"quesma/quesma/gzip"
	"quesma/quesma/mux"
	"quesma/quesma/recovery"
	"quesma/quesma/routes"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/table_resolver"
	"quesma/telemetry"
	"quesma/tracing"
	"quesma/util"
	"strings"
	"sync/atomic"
	"time"
)

type (
	Quesma struct {
		processor               engine
		publicTcpPort           util.Port
		quesmaManagementConsole *ui.QuesmaManagementConsole
		config                  *config.QuesmaConfiguration
		telemetryAgent          telemetry.PhoneHomeAgent
	}
	engine interface {
		Ingest()
		Stop(ctx context.Context)
	}
)

func responseFromElastic(ctx context.Context, elkResponse *http.Response, w http.ResponseWriter) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Elasticsearch")

	copyHeaders(w, elkResponse)
	w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
	// io.Copy calls WriteHeader implicitly
	w.WriteHeader(elkResponse.StatusCode)
	if _, err := io.Copy(w, elkResponse.Body); err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error copying response body: %v", err)
		http.Error(w, "Error copying response body", http.StatusInternalServerError)
		return
	}
	elkResponse.Body.Close()
}

func responseFromQuesma(ctx context.Context, unzipped []byte, w http.ResponseWriter, quesmaResponse *mux.Result, zip bool) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Quesma")

	for key, value := range quesmaResponse.Meta {
		w.Header().Set(key, value)
	}
	if zip {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.Header().Set(quesmaSourceHeader, quesmaSourceClickhouse)
	w.WriteHeader(quesmaResponse.StatusCode)
	if zip {
		zipped, err := gzip.Zip(unzipped)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("Error zipping: %v", err)
		}
		_, _ = io.Copy(w, bytes.NewBuffer(zipped))
	} else {
		_, _ = io.Copy(w, bytes.NewBuffer(unzipped))
	}
}

func NewQuesmaTcpProxy(phoneHomeAgent telemetry.PhoneHomeAgent, config *config.QuesmaConfiguration, quesmaManagementConsole *ui.QuesmaManagementConsole, logChan <-chan logger.LogWithLevel, inspect bool) *Quesma {
	return &Quesma{
		processor:               proxy.NewTcpProxy(config.PublicTcpPort, config.Elasticsearch.Url.Host, inspect),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func NewHttpProxy(phoneHomeAgent telemetry.PhoneHomeAgent, logManager *clickhouse.LogManager, ingestProcessor *ingest.IngestProcessor, schemaLoader clickhouse.TableDiscovery,
	indexManager elasticsearch.IndexManagement, schemaRegistry schema.Registry, config *config.QuesmaConfiguration,
	quesmaManagementConsole *ui.QuesmaManagementConsole, abResultsRepository ab_testing.Sender, resolver table_resolver.TableResolver) *Quesma {
	queryRunner := NewQueryRunner(logManager, config, indexManager, quesmaManagementConsole, schemaRegistry, abResultsRepository, resolver)

	// not sure how we should configure our query translator ???
	// is this a config option??

	queryRunner.DateMathRenderer = queryparser.DateMathExpressionFormatLiteral

	// tests should not be run with optimization enabled by default
	queryRunner.EnableQueryOptimization(config)

	return &Quesma{
		telemetryAgent:          phoneHomeAgent,
		processor:               newDualWriteProxy(schemaLoader, logManager, indexManager, schemaRegistry, config, quesmaManagementConsole, phoneHomeAgent, queryRunner, ingestProcessor, resolver),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

type router struct {
	config                  *config.QuesmaConfiguration
	requestPreprocessors    processorChain
	quesmaManagementConsole *ui.QuesmaManagementConsole
	phoneHomeAgent          telemetry.PhoneHomeAgent
	httpClient              *http.Client
	failedRequests          atomic.Int64
}

func (r *router) registerPreprocessor(preprocessor RequestPreprocessor) {
	r.requestPreprocessors = append(r.requestPreprocessors, preprocessor)
}

func (r *router) errorResponse(ctx context.Context, err error, w http.ResponseWriter) {
	r.failedRequests.Add(1)

	msg := "Internal Quesma Error.\nPlease contact support if the problem persists."
	reason := "Failed request."
	result := mux.ServerErrorResult()

	// if error is an error with user-friendly message, we should use it
	var endUserError *end_user_errors.EndUserError
	if errors.As(err, &endUserError) {
		msg = endUserError.EndUserErrorMessage()
		reason = endUserError.Reason()

		// we treat all `Q1xxx` errors as bad requests here
		if endUserError.ErrorType().Number < 2000 {
			result = mux.BadReqeustResult()
		}
	}

	logger.ErrorWithCtxAndReason(ctx, reason).Msgf("quesma request failed: %v", err)

	requestId := "n/a"
	if contextRid, ok := ctx.Value(tracing.RequestIdCtxKey).(string); ok {
		requestId = contextRid
	}

	// We should not send our error message to the client. There can be sensitive information in it.
	// We will send ID of failed request instead
	responseFromQuesma(ctx, []byte(fmt.Sprintf("%s\nRequest ID: %s\n", msg, requestId)), w, result, false)
}

func (*router) closedIndexResponse(ctx context.Context, w http.ResponseWriter, pattern string) {
	// TODO we should return a proper status code here (400?)
	w.WriteHeader(http.StatusOK)

	response := make(types.JSON)

	response["error"] = queryparser.Error{
		RootCause: []queryparser.RootCause{
			{
				Type:   "index_closed_exception",
				Reason: fmt.Sprintf("pattern %s is not routed to any connector", pattern),
			},
		},
		Type:   "index_closed_exception",
		Reason: fmt.Sprintf("pattern %s is not routed to any connector", pattern),
	}

	b, err := response.Bytes()
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error marshalling response: %v", err)
		return
	}

	w.Write(b)

}

func (r *router) reroute(ctx context.Context, w http.ResponseWriter, req *http.Request, reqBody []byte, router *mux.PathRouter, logManager *clickhouse.LogManager) {
	defer recovery.LogAndHandlePanic(ctx, func(err error) {
		w.WriteHeader(500)
		w.Write(queryparser.InternalQuesmaError("Unknown Quesma error"))
	})

	quesmaRequest, ctx, err := r.preprocessRequest(ctx, &mux.Request{
		Method:      req.Method,
		Path:        strings.TrimSuffix(req.URL.Path, "/"),
		Params:      map[string]string{},
		Headers:     req.Header,
		QueryParams: req.URL.Query(),
		Body:        string(reqBody),
	})

	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error preprocessing request: %v", err)
	}

	quesmaRequest.ParsedBody = types.ParseRequestBody(quesmaRequest.Body)

	handler, decision := router.Matches(quesmaRequest)

	if decision != nil {
		w.Header().Set(quesmaTableResolverHeader, decision.String())
	} else {
		w.Header().Set(quesmaTableResolverHeader, "n/a")
	}

	if handler != nil {
		quesmaResponse, err := recordRequestToClickhouse(req.URL.Path, r.quesmaManagementConsole, func() (*mux.Result, error) {
			return handler(ctx, quesmaRequest)
		})

		zip := strings.Contains(req.Header.Get("Accept-Encoding"), "gzip")

		if err == nil {
			logger.Debug().Ctx(ctx).Msg("responding from quesma")
			unzipped := []byte{}
			if quesmaResponse != nil {
				unzipped = []byte(quesmaResponse.Body)
			}
			if len(unzipped) == 0 {
				logger.WarnWithCtx(ctx).Msgf("empty response from Clickhouse, method=%s", req.Method)
			}
			addProductAndContentHeaders(req.Header, w.Header())

			responseFromQuesma(ctx, unzipped, w, quesmaResponse, zip)

		} else {
			r.errorResponse(ctx, err, w)
		}
	} else {

		var sendToElastic bool

		if decision != nil {

			if decision.Err != nil {
				w.Header().Set(quesmaSourceHeader, quesmaSourceClickhouse)
				addProductAndContentHeaders(req.Header, w.Header())
				r.errorResponse(ctx, decision.Err, w)
				return
			}

			if decision.IsClosed {
				w.Header().Set(quesmaSourceHeader, quesmaSourceClickhouse)
				addProductAndContentHeaders(req.Header, w.Header())
				r.closedIndexResponse(ctx, w, decision.IndexPattern)
				return
			}

			if decision.IsEmpty {
				w.Header().Set(quesmaSourceHeader, quesmaSourceClickhouse)
				addProductAndContentHeaders(req.Header, w.Header())
				w.WriteHeader(http.StatusNoContent)
				w.Write(queryparser.EmptySearchResponse(ctx))
				return
			}

			for _, connector := range decision.UseConnectors {
				if _, ok := connector.(*table_resolver.ConnectorDecisionElastic); ok {
					// this is desired elastic call
					sendToElastic = true
					break
				}
			}

		} else {
			// this is fallback case
			// in case we don't support sth, we should send it to Elastic
			sendToElastic = true
		}

		if sendToElastic {
			feature.AnalyzeUnsupportedCalls(ctx, req.Method, req.URL.Path, req.Header.Get(opaqueIdHeaderKey), logManager.ResolveIndexPattern)

			rawResponse := <-r.sendHttpRequestToElastic(ctx, req, reqBody, true)
			response := rawResponse.response
			if response != nil {
				responseFromElastic(ctx, response, w)
			} else {
				w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
				w.WriteHeader(500)
				if rawResponse.error != nil {
					_, _ = w.Write([]byte(rawResponse.error.Error()))
				}
			}
		} else {
			r.errorResponse(ctx, end_user_errors.ErrNoConnector.New(fmt.Errorf("no connector found")), w)
		}
	}
}

func (r *router) preprocessRequest(ctx context.Context, quesmaRequest *mux.Request) (*mux.Request, context.Context, error) {
	var err error
	var processedRequest = quesmaRequest
	for _, preprocessor := range r.requestPreprocessors {
		ctx, processedRequest, err = preprocessor.PreprocessRequest(ctx, processedRequest)
		if err != nil {
			return nil, nil, err
		}
	}
	return processedRequest, ctx, nil
}

type elasticResult struct {
	response *http.Response
	error    error
	took     time.Duration
}

func (r *router) sendHttpRequestToElastic(ctx context.Context, req *http.Request,
	reqBody []byte, isManagement bool) chan elasticResult {
	elkResponseChan := make(chan elasticResult)

	// If Quesma is exposing unauthenticated API but underlying Elasticsearch requires authentication, we should add the
	if r.config.DisableAuth && req.Header.Get("Authorization") == "" && r.config.Elasticsearch.User != "" {
		logger.DebugWithCtx(ctx).Msgf("path=%s routed to Elasticsearch, need add auth header to the request", req.URL)
		req.SetBasicAuth(r.config.Elasticsearch.User, r.config.Elasticsearch.Password)
	}

	if req.Header.Get("Authorization") != "" {
		var userName string
		if user, err := util.ExtractUsernameFromBasicAuthHeader(req.Header.Get("Authorization")); err == nil {
			userName = user
		} else {
			logger.Warn().Msgf("Failed to extract username from auth header: %v", err)
		}
		logger.DebugWithCtx(ctx).Msgf("[AUTH] [%s] routed to Elasticsearch, called by user [%s]", req.URL, userName)
	}

	go func() {
		elkResponseChan <- recordRequestToElastic(req.URL.Path, r.quesmaManagementConsole, func() elasticResult {

			isWrite := elasticsearch.IsWriteRequest(req)

			var span telemetry.Span
			if isManagement {
				if isWrite {
					span = r.phoneHomeAgent.ElasticBypassedWriteRequestsDuration().Begin()
				} else {
					span = r.phoneHomeAgent.ElasticBypassedReadRequestsDuration().Begin()
				}
			} else {
				if isWrite {
					span = r.phoneHomeAgent.ElasticWriteRequestsDuration().Begin()
				} else {
					span = r.phoneHomeAgent.ElasticReadRequestsDuration().Begin()
				}
			}

			resp, err := r.sendHttpRequest(ctx, r.config.Elasticsearch.Url.String(), req, reqBody)
			took := span.End(err)
			return elasticResult{resp, err, took}
		})
	}()
	return elkResponseChan
}

func isResponseOk(resp *http.Response) bool {
	return resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 500
}

func isIngest(path string) bool {
	return strings.HasSuffix(path, routes.BulkPath) // We may add more methods in future such as `_put` or `_create`
}

func recordRequestToClickhouse(path string, qmc *ui.QuesmaManagementConsole, requestFunc func() (*mux.Result, error)) (*mux.Result, error) {
	statName := ui.RequestStatisticKibana2Clickhouse
	if isIngest(path) {
		statName = ui.RequestStatisticIngest2Clickhouse
	}
	now := time.Now()
	response, err := requestFunc()
	qmc.RecordRequest(statName, time.Since(now), err != nil)
	return response, err
}

func recordRequestToElastic(path string, qmc *ui.QuesmaManagementConsole, requestFunc func() elasticResult) elasticResult {
	statName := ui.RequestStatisticKibana2Elasticsearch
	if isIngest(path) {
		statName = ui.RequestStatisticIngest2Elasticsearch
	}
	now := time.Now()
	response := requestFunc()
	qmc.RecordRequest(statName, time.Since(now), !isResponseOk(response.response))
	return response
}

func peekBody(r *http.Request) ([]byte, error) {
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		logger.ErrorWithCtxAndReason(r.Context(), "incomplete request").
			Msgf("Error reading request body: %v", err)
		return nil, err
	}

	contentEncoding := r.Header.Get("Content-Encoding")
	switch contentEncoding {
	case "":
		// No compression, leaving reqBody as-is
	case "gzip":
		reqBody, err = gzip.UnZip(reqBody)
		if err != nil {
			logger.ErrorWithCtxAndReason(r.Context(), "invalid gzip body").
				Msgf("Error decompressing gzip body: %v", err)
			return nil, err
		}
	default:
		logger.ErrorWithCtxAndReason(r.Context(), "unsupported Content-Encoding type").
			Msgf("Unsupported Content-Encoding type: %s", contentEncoding)
		return nil, errors.New("unsupported Content-Encoding type")
	}
	r.Header.Del("Content-Encoding") // In the transparent proxy case we will send an uncompressed body, so the header should be removed

	r.Body = io.NopCloser(bytes.NewBuffer(reqBody))
	return reqBody, nil
}

func copyHeaders(w http.ResponseWriter, elkResponse *http.Response) {
	for key, values := range elkResponse.Header {
		for _, value := range values {
			if key != httpHeaderContentLength {
				if w.Header().Get(key) == "" {
					w.Header().Add(key, value)
				}
			}
		}
	}
}

func (q *Quesma) Close(ctx context.Context) {
	q.processor.Stop(ctx)
}

func (q *Quesma) Start() {
	defer recovery.LogPanic()
	logger.Info().Msgf("starting quesma, transparent proxy mode: %t", q.config.TransparentProxy)

	go q.processor.Ingest()
	go q.quesmaManagementConsole.Run()
}

func (r *router) sendHttpRequest(ctx context.Context, address string, originalReq *http.Request, originalReqBody []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, originalReq.Method, address+originalReq.URL.String(), bytes.NewBuffer(originalReqBody))

	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error creating request: %v", err)
		return nil, err
	}

	req.Header = originalReq.Header

	resp, err := r.httpClient.Do(req)
	if err != nil {
		logger.ErrorWithCtxAndReason(ctx, "No network connection").
			Msgf("Error sending request: %v", err)
		return nil, err
	}

	return resp, nil
}
