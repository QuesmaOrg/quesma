// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"bytes"
	"context"
	"crypto/tls"
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
	"quesma/queryparser"
	"quesma/quesma/async_search_storage"
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
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const concurrentClientsLimitV2 = 100 // FIXME this should be configurable

type simultaneousClientsLimiterV2 struct {
	counter atomic.Int64
	handler http.Handler
	limit   int64
}

func newSimultaneousClientsLimiterV2(handler http.Handler, limit int64) *simultaneousClientsLimiterV2 {
	return &simultaneousClientsLimiterV2{
		handler: handler,
		limit:   limit,
	}
}

func (c *simultaneousClientsLimiterV2) ServeHTTP(w http.ResponseWriter, r *http.Request) {

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

type dualWriteHttpProxyV2 struct {
	routingHttpServer   *http.Server
	indexManagement     elasticsearch.IndexManagement
	logManager          *clickhouse.LogManager
	publicPort          util.Port
	asyncQueriesEvictor *async_search_storage.AsyncQueriesEvictor
	queryRunner         *QueryRunner
	schemaRegistry      schema.Registry
	schemaLoader        clickhouse.TableDiscovery
}

func (q *dualWriteHttpProxyV2) Stop(ctx context.Context) {
	q.Close(ctx)
}

func newDualWriteProxyV2(schemaLoader clickhouse.TableDiscovery, logManager *clickhouse.LogManager, indexManager elasticsearch.IndexManagement, registry schema.Registry, config *config.QuesmaConfiguration, quesmaManagementConsole *ui.QuesmaManagementConsole, agent telemetry.PhoneHomeAgent, processor *ingest.IngestProcessor, resolver table_resolver.TableResolver, abResultsRepository ab_testing.Sender) *dualWriteHttpProxyV2 {
	queryRunner := NewQueryRunner(logManager, config, indexManager, quesmaManagementConsole, registry, abResultsRepository, resolver)
	// not sure how we should configure our query translator ???
	// is this a config option??

	queryRunner.DateMathRenderer = queryparser.DateMathExpressionFormatLiteral

	// tests should not be run with optimization enabled by default
	queryRunner.EnableQueryOptimization(config)

	ingestRouter := ConfigureIngestRouterV2(config, processor, agent, resolver)
	searchRouter := ConfigureSearchRouterV2(config, registry, logManager, quesmaManagementConsole, queryRunner, resolver)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   time.Minute, // should be more configurable, 30s is Kibana default timeout
	}
	routerInstance := routerV2{phoneHomeAgent: agent, config: config, quesmaManagementConsole: quesmaManagementConsole, httpClient: client, requestPreprocessors: processorChain{}}
	routerInstance.
		registerPreprocessor(NewTraceIdPreprocessor())

	agent.FailedRequestsCollector(func() int64 {
		return routerInstance.failedRequests.Load()
	})

	elasticHttpFrontentConnector := NewElasticHttpFrontendConnector(":"+strconv.Itoa(int(config.PublicTcpPort)),
		&routerInstance, searchRouter, ingestRouter, logManager, agent)

	var limitedHandler http.Handler
	if config.DisableAuth {
		limitedHandler = newSimultaneousClientsLimiterV2(elasticHttpFrontentConnector, concurrentClientsLimitV2)
	} else {
		limitedHandler = newSimultaneousClientsLimiterV2(NewAuthMiddleware(elasticHttpFrontentConnector, config.Elasticsearch), concurrentClientsLimitV2)
	}

	return &dualWriteHttpProxyV2{
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

func (q *dualWriteHttpProxyV2) Close(ctx context.Context) {
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

func (q *dualWriteHttpProxyV2) Ingest() {
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

func responseFromElasticV2(ctx context.Context, elkResponse *http.Response, w http.ResponseWriter) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Elasticsearch")

	copyHeadersV2(w, elkResponse)
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

func responseFromQuesmaV2(ctx context.Context, unzipped []byte, w http.ResponseWriter, quesmaResponse *mux.Result, zip bool) {
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

type routerV2 struct {
	config                  *config.QuesmaConfiguration
	requestPreprocessors    processorChain
	quesmaManagementConsole *ui.QuesmaManagementConsole
	phoneHomeAgent          telemetry.PhoneHomeAgent
	httpClient              *http.Client
	failedRequests          atomic.Int64
}

func (r *routerV2) registerPreprocessor(preprocessor RequestPreprocessor) {
	r.requestPreprocessors = append(r.requestPreprocessors, preprocessor)
}

func (r *routerV2) errorResponseV2(ctx context.Context, err error, w http.ResponseWriter) {
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
	responseFromQuesmaV2(ctx, []byte(fmt.Sprintf("%s\nRequest ID: %s\n", msg, requestId)), w, result, false)
}

func (*routerV2) closedIndexResponse(ctx context.Context, w http.ResponseWriter, pattern string) {
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

func (r *routerV2) elasticFallback(decision *table_resolver.Decision,
	ctx context.Context, w http.ResponseWriter,
	req *http.Request, reqBody []byte, logManager *clickhouse.LogManager) {

	var sendToElastic bool

	if decision != nil {

		if decision.Err != nil {
			w.Header().Set(quesmaSourceHeader, quesmaSourceClickhouse)
			addProductAndContentHeaders(req.Header, w.Header())
			r.errorResponseV2(ctx, decision.Err, w)
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
			responseFromElasticV2(ctx, response, w)
		} else {
			w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
			w.WriteHeader(500)
			if rawResponse.error != nil {
				_, _ = w.Write([]byte(rawResponse.error.Error()))
			}
		}
	} else {
		r.errorResponseV2(ctx, end_user_errors.ErrNoConnector.New(fmt.Errorf("no connector found")), w)
	}
}

func (r *routerV2) executeHandlerWithoutFallback(decision *table_resolver.Decision, ctx context.Context, w http.ResponseWriter, req *http.Request, reqBody []byte, handler mux.Handler, quesmaRequest *mux.Request, logManager *clickhouse.LogManager) bool {
	if decision != nil {
		w.Header().Set(quesmaTableResolverHeader, decision.String())
	} else {
		w.Header().Set(quesmaTableResolverHeader, "n/a")
	}

	if handler == nil {
		return false
	}

	quesmaResponse, err := recordRequestToClickhouseV2(req.URL.Path, r.quesmaManagementConsole, func() (*mux.Result, error) {
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
		r.errorResponseV2(ctx, err, w)
	}
	return true
}

func (r *routerV2) reroute(ctx context.Context, w http.ResponseWriter, req *http.Request, reqBody []byte, searchRouter *mux.PathRouter, ingestRouter *mux.PathRouter, logManager *clickhouse.LogManager) {
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
	var handler mux.Handler
	var decision *table_resolver.Decision
	searchHandler, searchDecision := searchRouter.Matches(quesmaRequest)
	if searchDecision != nil {
		decision = searchDecision
	}
	if searchHandler != nil {
		handler = searchHandler
	}
	respFromQuesma := r.executeHandlerWithoutFallback(decision, ctx, w, req, reqBody, handler, quesmaRequest, logManager)
	if !respFromQuesma {
		ingestHandler, ingestDecision := ingestRouter.Matches(quesmaRequest)
		if searchDecision == nil {
			decision = ingestDecision
		}
		if searchHandler == nil {
			handler = ingestHandler
		}
		respFromQuesma = r.executeHandlerWithoutFallback(decision, ctx, w, req, reqBody, handler, quesmaRequest, logManager)
		if !respFromQuesma {
			r.elasticFallback(decision, ctx, w, req, reqBody, logManager)
		}
	}
}

func (r *routerV2) preprocessRequest(ctx context.Context, quesmaRequest *mux.Request) (*mux.Request, context.Context, error) {
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

type elasticResultV2 struct {
	response *http.Response
	error    error
	took     time.Duration
}

func (r *routerV2) sendHttpRequestToElastic(ctx context.Context, req *http.Request,
	reqBody []byte, isManagement bool) chan elasticResultV2 {
	elkResponseChan := make(chan elasticResultV2)

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
		elkResponseChan <- recordRequestToElasticV2(req.URL.Path, r.quesmaManagementConsole, func() elasticResultV2 {

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
			return elasticResultV2{resp, err, took}
		})
	}()
	return elkResponseChan
}

func isResponseOkV2(resp *http.Response) bool {
	return resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 500
}

func isIngestV2(path string) bool {
	return strings.HasSuffix(path, routes.BulkPath) // We may add more methods in future such as `_put` or `_create`
}

func recordRequestToClickhouseV2(path string, qmc *ui.QuesmaManagementConsole, requestFunc func() (*mux.Result, error)) (*mux.Result, error) {
	statName := ui.RequestStatisticKibana2Clickhouse
	if isIngestV2(path) {
		statName = ui.RequestStatisticIngest2Clickhouse
	}
	now := time.Now()
	response, err := requestFunc()
	qmc.RecordRequest(statName, time.Since(now), err != nil)
	return response, err
}

func recordRequestToElasticV2(path string, qmc *ui.QuesmaManagementConsole, requestFunc func() elasticResultV2) elasticResultV2 {
	statName := ui.RequestStatisticKibana2Elasticsearch
	if isIngest(path) {
		statName = ui.RequestStatisticIngest2Elasticsearch
	}
	now := time.Now()
	response := requestFunc()
	qmc.RecordRequest(statName, time.Since(now), !isResponseOkV2(response.response))
	return response
}

func peekBodyV2(r *http.Request) ([]byte, error) {
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

func copyHeadersV2(w http.ResponseWriter, elkResponse *http.Response) {
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

func (r *routerV2) sendHttpRequest(ctx context.Context, address string, originalReq *http.Request, originalReqBody []byte) (*http.Response, error) {
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
