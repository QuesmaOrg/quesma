// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/ab_testing"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/end_user_errors"
	"github.com/QuesmaOrg/quesma/quesma/feature"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/ingest"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/queryparser"
	"github.com/QuesmaOrg/quesma/quesma/quesma/async_search_storage"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/gzip"
	"github.com/QuesmaOrg/quesma/quesma/quesma/recovery"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	"github.com/QuesmaOrg/quesma/quesma/telemetry"
	"github.com/QuesmaOrg/quesma/quesma/util"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/diag"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/routes"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/tracing"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const concurrentClientsLimit = 100 // FIXME this should be configurable

type simultaneousClientsLimiter struct {
	counter atomic.Int64
	handler http.Handler
	limit   int64
}

func newSimultaneousClientsLimiter(handler http.Handler, limit int64) *simultaneousClientsLimiter {
	return &simultaneousClientsLimiter{
		handler: handler,
		limit:   limit,
	}
}

func (c *simultaneousClientsLimiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	current := c.counter.Load()
	// this is hard limit, we should not allow to go over it
	if current >= c.limit {
		logger.ErrorWithCtx(r.Context()).Msgf("Too many requests. current: %d, limit: %d", current, c.limit)
		http.Error(w, "Too many requests", http.StatusTooManyRequests)
		return
	}

	c.counter.Add(1)
	defer c.counter.Add(-1)
	c.handler.ServeHTTP(w, r)
}

type dualWriteHttpProxy struct {
	routingHttpServer   *http.Server
	elasticRouter       *quesma_api.PathRouter
	indexManagement     elasticsearch.IndexManagement
	logManager          *clickhouse.LogManager
	publicPort          util.Port
	asyncQueriesEvictor *async_search_storage.AsyncQueriesEvictor
	queryRunner         *QueryRunner
	schemaRegistry      schema.Registry
	schemaLoader        clickhouse.TableDiscovery
}

func (q *dualWriteHttpProxy) Stop(ctx context.Context) {
	q.Close(ctx)
}

func newDualWriteProxy(schemaLoader clickhouse.TableDiscovery, logManager *clickhouse.LogManager, indexManager elasticsearch.IndexManagement, registry schema.Registry, config *config.QuesmaConfiguration, quesmaManagementConsole *ui.QuesmaManagementConsole, agent telemetry.PhoneHomeAgent, processor *ingest.IngestProcessor, resolver table_resolver.TableResolver, abResultsRepository ab_testing.Sender) *dualWriteHttpProxy {
	queryRunner := NewQueryRunner(logManager, config, indexManager, quesmaManagementConsole, registry, abResultsRepository, resolver, schemaLoader, model.DefaultSearchAfterStrategy)
	// not sure how we should configure our query translator ???
	// is this a config option??

	queryRunner.DateMathRenderer = queryparser.DateMathExpressionFormatLiteral

	// tests should not be run with optimization enabled by default
	queryRunner.EnableQueryOptimization(config)

	esBackendConnector := backend_connectors.NewElasticsearchBackendConnector(config.Elasticsearch)
	pathRouter := ConfigureRouter(config, registry, logManager, processor, quesmaManagementConsole, agent, queryRunner, resolver, esBackendConnector)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   time.Minute, // should be more configurable, 30s is Kibana default timeout
	}
	routerInstance := router{phoneHomeAgent: agent, config: config, quesmaManagementConsole: quesmaManagementConsole, httpClient: client, requestPreprocessors: quesma_api.ProcessorChain{}}
	routerInstance.
		registerPreprocessor(quesma_api.NewTraceIdPreprocessor())

	agent.FailedRequestsCollector(func() int64 {
		return routerInstance.failedRequests.Load()
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer recovery.LogPanic()
		reqBody, err := peekBody(req)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}

		ua := req.Header.Get("User-Agent")
		agent.UserAgentCounters().Add(ua, 1)

		routerInstance.reroute(req.Context(), w, req, reqBody, pathRouter, logManager, registry)
	})
	var limitedHandler http.Handler
	if config.DisableAuth {
		limitedHandler = newSimultaneousClientsLimiter(handler, concurrentClientsLimit)
	} else {
		limitedHandler = newSimultaneousClientsLimiter(NewAuthMiddleware(handler, config.Elasticsearch), concurrentClientsLimit)
	}

	return &dualWriteHttpProxy{
		elasticRouter:  pathRouter,
		schemaRegistry: registry,
		schemaLoader:   schemaLoader,
		routingHttpServer: &http.Server{
			Addr:    ":" + strconv.Itoa(int(config.PublicTcpPort)),
			Handler: limitedHandler,
		},
		indexManagement: indexManager,
		logManager:      logManager,
		publicPort:      config.PublicTcpPort,
		asyncQueriesEvictor: async_search_storage.NewAsyncQueriesEvictor(
			queryRunner.AsyncRequestStorage.(async_search_storage.AsyncSearchStorageInMemory),
			queryRunner.AsyncQueriesContexts.(async_search_storage.AsyncQueryContextStorageInMemory),
		),
		queryRunner: queryRunner,
	}
}

func (q *dualWriteHttpProxy) Close(ctx context.Context) {
	if q.logManager != nil {
		defer q.logManager.Close()
	}
	if q.queryRunner != nil {
		q.queryRunner.Close()
	}
	if q.asyncQueriesEvictor != nil {
		q.asyncQueriesEvictor.Close()
	}
	if err := q.routingHttpServer.Shutdown(ctx); err != nil {
		logger.Fatal().Msgf("Error during server shutdown: %v", err)
	}
}

func (q *dualWriteHttpProxy) Ingest() {
	q.schemaLoader.ReloadTableDefinitions()
	q.logManager.Start()
	q.indexManagement.Start()
	go q.asyncQueriesEvictor.AsyncQueriesGC()
	go func() {
		if err := q.routingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Msgf("Error starting http server: %v", err)
		}
		logger.Info().Msgf("Accepting HTTP at :%d", q.publicPort)
	}()
}

func responseFromElastic(ctx context.Context, elkResponse *http.Response, w http.ResponseWriter) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Elasticsearch")

	copyHeaders(w, elkResponse)
	w.Header().Set(frontend_connectors.QuesmaSourceHeader, frontend_connectors.QuesmaSourceElastic)
	// io.Copy calls WriteHeader implicitly
	w.WriteHeader(elkResponse.StatusCode)
	if _, err := io.Copy(w, elkResponse.Body); err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error copying response body: %v", err)
		http.Error(w, "Error copying response body", http.StatusInternalServerError)
		return
	}
	elkResponse.Body.Close()
}

func responseFromQuesma(ctx context.Context, unzipped []byte, w http.ResponseWriter, quesmaResponse *quesma_api.Result, zip bool) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Quesma")

	for key, value := range quesmaResponse.Meta {
		w.Header().Set(key, value.(string))
	}
	if zip {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.Header().Set(frontend_connectors.QuesmaSourceHeader, frontend_connectors.QuesmaSourceClickhouse)
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

type router struct {
	config                  *config.QuesmaConfiguration
	requestPreprocessors    quesma_api.ProcessorChain
	quesmaManagementConsole *ui.QuesmaManagementConsole
	phoneHomeAgent          telemetry.PhoneHomeAgent
	httpClient              *http.Client
	failedRequests          atomic.Int64
}

func (r *router) registerPreprocessor(preprocessor quesma_api.RequestPreprocessor) {
	r.requestPreprocessors = append(r.requestPreprocessors, preprocessor)
}

func (r *router) errorResponse(ctx context.Context, err error, w http.ResponseWriter) {
	r.failedRequests.Add(1)

	msg := "Internal Quesma Error.\nPlease contact support if the problem persists."
	reason := "Failed request."
	result := quesma_api.ServerErrorResult()

	// if error is an error with user-friendly message, we should use it
	var endUserError *end_user_errors.EndUserError
	if errors.As(err, &endUserError) {
		msg = endUserError.EndUserErrorMessage()
		reason = endUserError.Reason()

		// we treat all `Q1xxx` errors as bad requests here
		if endUserError.ErrorType().Number < 2000 {
			result = quesma_api.BadReqeustResult()
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

func (r *router) reroute(ctx context.Context, w http.ResponseWriter, req *http.Request, reqBody []byte, router *quesma_api.PathRouter, logManager *clickhouse.LogManager, schemaRegistry schema.Registry) {
	defer recovery.LogAndHandlePanic(ctx, func(err error) {
		w.WriteHeader(500)
		w.Write(queryparser.InternalQuesmaError("Unknown Quesma error"))
	})

	quesmaRequest, ctx, err := r.preprocessRequest(ctx, &quesma_api.Request{
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

	handlersPipe, decision := router.Matches(quesmaRequest)

	if decision != nil {
		w.Header().Set(frontend_connectors.QuesmaTableResolverHeader, decision.String())
	} else {
		w.Header().Set(frontend_connectors.QuesmaTableResolverHeader, "n/a")
	}

	if handlersPipe != nil {
		quesmaResponse, err := recordRequestToClickhouse(req.URL.Path, r.quesmaManagementConsole, func() (*quesma_api.Result, error) {
			return handlersPipe.Handler(ctx, quesmaRequest, w)
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
			frontend_connectors.AddProductAndContentHeaders(req.Header, w.Header())

			responseFromQuesma(ctx, unzipped, w, quesmaResponse, zip)

		} else {
			r.errorResponse(ctx, err, w)
		}
	} else {

		var sendToElastic bool

		if decision != nil {

			if decision.Err != nil {
				w.Header().Set(frontend_connectors.QuesmaSourceHeader, frontend_connectors.QuesmaSourceClickhouse)
				frontend_connectors.AddProductAndContentHeaders(req.Header, w.Header())
				r.errorResponse(ctx, decision.Err, w)
				return
			}

			if decision.IsClosed {
				w.Header().Set(frontend_connectors.QuesmaSourceHeader, frontend_connectors.QuesmaSourceClickhouse)
				frontend_connectors.AddProductAndContentHeaders(req.Header, w.Header())
				r.closedIndexResponse(ctx, w, decision.IndexPattern)
				return
			}

			if decision.IsEmpty {
				w.Header().Set(frontend_connectors.QuesmaSourceHeader, frontend_connectors.QuesmaSourceClickhouse)
				frontend_connectors.AddProductAndContentHeaders(req.Header, w.Header())
				w.WriteHeader(http.StatusNoContent)
				w.Write(queryparser.EmptySearchResponse(ctx))
				return
			}

			for _, connector := range decision.UseConnectors {
				if _, ok := connector.(*quesma_api.ConnectorDecisionElastic); ok {
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
			resolveIndexPattern := func(ctx context.Context, pattern string) ([]string, error) {
				return logManager.ResolveIndexPattern(ctx, schemaRegistry, pattern)
			}
			feature.AnalyzeUnsupportedCalls(ctx, req.Method, req.URL.Path, req.Header.Get(frontend_connectors.OpaqueIdHeaderKey), resolveIndexPattern)

			rawResponse := <-r.sendHttpRequestToElastic(ctx, req, reqBody, true)
			response := rawResponse.response
			if response != nil {
				responseFromElastic(ctx, response, w)
			} else {
				w.Header().Set(frontend_connectors.QuesmaSourceHeader, frontend_connectors.QuesmaSourceElastic)
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

func (r *router) preprocessRequest(ctx context.Context, quesmaRequest *quesma_api.Request) (*quesma_api.Request, context.Context, error) {
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

			var span diag.Span
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

func recordRequestToClickhouse(path string, qmc *ui.QuesmaManagementConsole, requestFunc func() (*quesma_api.Result, error)) (*quesma_api.Result, error) {
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
			if key != frontend_connectors.HttpHeaderContentLength {
				if w.Header().Get(key) == "" {
					w.Header().Add(key, value)
				}
			}
		}
	}
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
