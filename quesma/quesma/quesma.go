package quesma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/end_user_errors"
	"mitmproxy/quesma/feature"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/proxy"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/gzip"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/routes"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/schema"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/tracing"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type (
	Quesma struct {
		processor               engine
		publicTcpPort           network.Port
		quesmaManagementConsole *ui.QuesmaManagementConsole
		config                  config.QuesmaConfiguration
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

func responseFromQuesma(ctx context.Context, unzipped []byte, w http.ResponseWriter, elkResponse *http.Response, quesmaResponse *mux.Result, zip bool) {
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
	if elkResponse != nil {
		LogMissingEsHeaders(elkResponse.Header, w.Header(), id)
	}
}

func sendElkResponseToQuesmaConsole(ctx context.Context, elkResponse elasticResult, console *ui.QuesmaManagementConsole) {
	reader := elkResponse.response.Body
	body, err := io.ReadAll(reader)
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error reading response body: %v", err)
		return
	}
	elkResponse.response.Body = io.NopCloser(bytes.NewBuffer(body))

	if gzip.IsGzipped(elkResponse.response) {
		body, err = gzip.UnZip(body)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("Error unzipping: %v", err)
		}
	}
	console.PushPrimaryInfo(&ui.QueryDebugPrimarySource{Id: id, QueryResp: body, PrimaryTook: elkResponse.took})
}

func NewQuesmaTcpProxy(phoneHomeAgent telemetry.PhoneHomeAgent, config config.QuesmaConfiguration, logChan <-chan tracing.LogWithLevel, inspect bool) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, nil, nil, logChan, phoneHomeAgent)
	return &Quesma{
		processor:               proxy.NewTcpProxy(config.PublicTcpPort, config.Elasticsearch.Url.Host, inspect),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func NewHttpProxy(phoneHomeAgent telemetry.PhoneHomeAgent, logManager *clickhouse.LogManager, schemaLoader clickhouse.TableDiscovery, indexManager elasticsearch.IndexManagement, schemaRegistry schema.Registry, config config.QuesmaConfiguration, logChan <-chan tracing.LogWithLevel) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logManager, indexManager, logChan, phoneHomeAgent)
	queryRunner := NewQueryRunner(logManager, config, indexManager, quesmaManagementConsole)

	// not sure how we should configure our query translator ???
	// is this a config option??

	queryRunner.DateMathRenderer = queryparser.DateMathExpressionFormatLiteral

	router := configureRouter(config, logManager, quesmaManagementConsole, phoneHomeAgent, queryRunner)
	return &Quesma{
		telemetryAgent:          phoneHomeAgent,
		processor:               newDualWriteProxy(schemaLoader, logManager, indexManager, schemaRegistry, config, router, quesmaManagementConsole, phoneHomeAgent, queryRunner),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

type router struct {
	config                  config.QuesmaConfiguration
	requestPreprocessors    processorChain
	quesmaManagementConsole *ui.QuesmaManagementConsole
	phoneHomeAgent          telemetry.PhoneHomeAgent
	httpClient              *http.Client
	failedRequests          atomic.Int64
}

func (r *router) registerPreprocessor(preprocessor RequestPreprocessor) {
	r.requestPreprocessors = append(r.requestPreprocessors, preprocessor)
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

	handler, found := router.Matches(quesmaRequest)
	if found {

		var elkResponseChan = make(chan elasticResult)

		if r.config.Elasticsearch.Call {
			elkResponseChan = r.sendHttpRequestToElastic(ctx, req, reqBody, false)
		}

		quesmaResponse, err := recordRequestToClickhouse(req.URL.Path, r.quesmaManagementConsole, func() (*mux.Result, error) {
			return handler(ctx, quesmaRequest)
		})
		var elkRawResponse elasticResult
		var elkResponse *http.Response
		if r.config.Elasticsearch.Call {
			elkRawResponse = <-elkResponseChan
			elkResponse = elkRawResponse.response
		} else {
			elkResponse = nil
		}
		if elkResponse != nil {
			if routes.IsQueryPath(req.URL.Path) { // We should send only responses for search queries to Quesma console
				sendElkResponseToQuesmaConsole(ctx, elkRawResponse, r.quesmaManagementConsole)
			}
			if !(elkResponse.StatusCode >= 200 && elkResponse.StatusCode < 300) {
				logger.WarnWithCtx(ctx).Msgf("Elasticsearch returned unexpected status code [%d] when calling [%s %s]", elkResponse.StatusCode, req.Method, req.URL.Path)
			}
		}

		zip := strings.Contains(req.Header.Get("Accept-Encoding"), "gzip")

		if err == nil {
			logger.Debug().Ctx(ctx).Msg("responding from quesma")
			unzipped := []byte{}
			if quesmaResponse != nil {
				unzipped = []byte(quesmaResponse.Body)
			}
			if len(unzipped) == 0 {
				logger.WarnWithCtx(ctx).Msg("empty response from Clickhouse")
			}
			addProductAndContentHeaders(req.Header, w.Header())

			responseFromQuesma(ctx, unzipped, w, elkResponse, quesmaResponse, zip)

		} else {

			r.failedRequests.Add(1)

			if elkResponse != nil && r.config.Mode == config.DualWriteQueryClickhouseFallback {
				logger.ErrorWithCtx(ctx).Msgf("quesma request failed: %v", err)
				responseFromElastic(ctx, elkResponse, w)

			} else {

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
				responseFromQuesma(ctx, []byte(fmt.Sprintf("%s\nRequest ID: %s\n", msg, requestId)), w, elkResponse, result, zip)
			}
		}
	} else {

		feature.AnalyzeUnsupportedCalls(ctx, req.Method, req.URL.Path, logManager.ResolveIndexes)

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
	}
}

func (r *router) preprocessRequest(ctx context.Context, quesmaRequest *mux.Request) (*mux.Request, context.Context, error) {
	var err error
	var processedRequest = quesmaRequest
	for _, preprocessor := range r.requestPreprocessors {
		if preprocessor.Applies(processedRequest) {
			ctx, processedRequest, err = preprocessor.PreprocessRequest(ctx, processedRequest)
			if err != nil {
				return nil, nil, err
			}
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

	// If the request is authenticated, we should not override it with the configured user
	if req.Header.Get("Authorization") == "" && r.config.Elasticsearch.User != "" {
		req.SetBasicAuth(r.config.Elasticsearch.User, r.config.Elasticsearch.Password)
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
	logger.Info().Msgf("starting quesma in the mode: %v", q.config.Mode)

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
