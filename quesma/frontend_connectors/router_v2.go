// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/feature"
	"quesma/logger"
	"quesma/queryparser"
	"quesma/quesma/config"
	"quesma/quesma/gzip"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/util"
	quesma_api "quesma_v2/core"
	"quesma_v2/core/diag"
	"quesma_v2/core/routes"
	"quesma_v2/core/tracing"
	"strings"
	"sync/atomic"
	"time"
)

func responseFromElasticV2(ctx context.Context, elkResponse *http.Response, w http.ResponseWriter) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Elasticsearch")

	copyHeadersV2(w, elkResponse)
	w.Header().Set(QuesmaSourceHeader, QuesmaSourceElastic)
	// io.Copy calls WriteHeader implicitly
	w.WriteHeader(elkResponse.StatusCode)
	if _, err := io.Copy(w, elkResponse.Body); err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error copying response body: %v", err)
		http.Error(w, "Error copying response body", http.StatusInternalServerError)
		return
	}
	elkResponse.Body.Close()
}

func responseFromQuesmaV2(ctx context.Context, unzipped []byte, w http.ResponseWriter, quesmaResponse *quesma_api.Result, zip bool) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Quesma")

	for key, value := range quesmaResponse.Meta {
		if headerStringValue, ok := value.(string); ok {
			w.Header().Set(key, headerStringValue)
		}
	}
	if zip {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.Header().Set(QuesmaSourceHeader, QuesmaSourceClickhouse)
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

type RouterV2 struct {
	Config               *config.QuesmaConfiguration
	RequestPreprocessors quesma_api.ProcessorChain

	HttpClient     *http.Client
	FailedRequests atomic.Int64

	debugInfoCollector diag.DebugInfoCollector
	phoneHomeAgent     diag.PhoneHomeClient
}

func (r *RouterV2) SetDependencies(deps quesma_api.Dependencies) {
	r.debugInfoCollector = deps.DebugInfoCollector()
	r.phoneHomeAgent = deps.PhoneHomeAgent()
}

func NewRouterV2(config *config.QuesmaConfiguration) *RouterV2 {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   time.Minute, // should be more configurable, 30s is Kibana default timeout
	}
	requestProcessors := quesma_api.ProcessorChain{}
	requestProcessors = append(requestProcessors, quesma_api.NewTraceIdPreprocessor())
	return &RouterV2{
		Config:               config,
		RequestPreprocessors: requestProcessors,
		HttpClient:           client,
	}
}

func (r *RouterV2) RegisterPreprocessor(preprocessor quesma_api.RequestPreprocessor) {
	r.RequestPreprocessors = append(r.RequestPreprocessors, preprocessor)
}

func (r *RouterV2) errorResponseV2(ctx context.Context, err error, w http.ResponseWriter) {
	r.FailedRequests.Add(1)

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
	responseFromQuesmaV2(ctx, []byte(fmt.Sprintf("%s\nRequest ID: %s\n", msg, requestId)), w, result, false)
}

func (*RouterV2) closedIndexResponse(ctx context.Context, w http.ResponseWriter, pattern string) {
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

func (r *RouterV2) ElasticFallback(decision *quesma_api.Decision,
	ctx context.Context, w http.ResponseWriter,
	req *http.Request, reqBody []byte, logManager *clickhouse.LogManager, schemaRegistry schema.Registry) {

	var sendToElastic bool

	if decision != nil {

		if decision.Err != nil {
			w.Header().Set(QuesmaSourceHeader, QuesmaSourceClickhouse)
			AddProductAndContentHeaders(req.Header, w.Header())
			r.errorResponseV2(ctx, decision.Err, w)
			return
		}

		if decision.IsClosed {
			w.Header().Set(QuesmaSourceHeader, QuesmaSourceClickhouse)
			AddProductAndContentHeaders(req.Header, w.Header())
			r.closedIndexResponse(ctx, w, decision.IndexPattern)
			return
		}

		if decision.IsEmpty {
			w.Header().Set(QuesmaSourceHeader, QuesmaSourceClickhouse)
			AddProductAndContentHeaders(req.Header, w.Header())
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
		if logManager != nil {
			resolveIndexPattern := func(ctx context.Context, pattern string) ([]string, error) {
				return logManager.ResolveIndexPattern(ctx, schemaRegistry, pattern)
			}
			feature.AnalyzeUnsupportedCalls(ctx, req.Method, req.URL.Path, req.Header.Get(OpaqueIdHeaderKey), resolveIndexPattern)
		}
		rawResponse := <-r.sendHttpRequestToElastic(ctx, req, reqBody, true)
		response := rawResponse.response
		if response != nil {
			responseFromElasticV2(ctx, response, w)
		} else {
			w.Header().Set(QuesmaSourceHeader, QuesmaSourceElastic)
			w.WriteHeader(500)
			if rawResponse.error != nil {
				_, _ = w.Write([]byte(rawResponse.error.Error()))
			}
		}
	} else {
		r.errorResponseV2(ctx, end_user_errors.ErrNoConnector.New(fmt.Errorf("no connector found")), w)
	}
}

func (r *RouterV2) Reroute(ctx context.Context, w http.ResponseWriter, req *http.Request, reqBody []byte, router quesma_api.Router, logManager *clickhouse.LogManager, schemaRegistry schema.Registry) {
	defer recovery.LogAndHandlePanic(ctx, func(err error) {
		w.WriteHeader(500)
		w.Write(queryparser.InternalQuesmaError("Unknown Quesma error"))
	})

	quesmaRequest, ctx, err := preprocessRequest(ctx, &quesma_api.Request{
		Method:          req.Method,
		Path:            strings.TrimSuffix(req.URL.Path, "/"),
		Params:          map[string]string{},
		Headers:         req.Header,
		QueryParams:     req.URL.Query(),
		Body:            string(reqBody),
		OriginalRequest: req,
	}, r.RequestPreprocessors)

	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error preprocessing request: %v", err)
	}

	quesmaRequest.ParsedBody = types.ParseRequestBody(quesmaRequest.Body)

	handlersPipe, decision := router.Matches(quesmaRequest)

	if decision != nil {
		w.Header().Set(QuesmaTableResolverHeader, decision.String())
	} else {
		w.Header().Set(QuesmaTableResolverHeader, "n/a")
	}
	dispatcher := &quesma_api.Dispatcher{}
	if handlersPipe != nil {
		quesmaResponse, err := recordRequestToClickhouseV2(req.URL.Path, r.debugInfoCollector, func() (*quesma_api.Result, error) {
			var result *quesma_api.Result
			result, err = handlersPipe.Handler(ctx, quesmaRequest)

			if result == nil {
				return result, err
			}
			metadata, message := dispatcher.Dispatch(handlersPipe.Processors, result.Meta, result.GenericResult)

			result = &quesma_api.Result{
				Body:          result.Body,
				Meta:          metadata,
				StatusCode:    result.StatusCode,
				GenericResult: message,
			}
			return result, err
		})

		zip := strings.Contains(req.Header.Get("Accept-Encoding"), "gzip")

		if err == nil {
			logger.Debug().Ctx(ctx).Msg("responding from quesma")
			unzipped := []byte{}
			if quesmaResponse != nil {
				unzipped = quesmaResponse.GenericResult.([]byte)
			}
			if len(unzipped) == 0 {
				logger.WarnWithCtx(ctx).Msgf("empty response from Clickhouse, method=%s", req.Method)
			}
			AddProductAndContentHeaders(req.Header, w.Header())

			responseFromQuesmaV2(ctx, unzipped, w, quesmaResponse, zip)

		} else {
			r.errorResponseV2(ctx, err, w)
		}
	} else {
		r.ElasticFallback(decision, ctx, w, req, reqBody, logManager, schemaRegistry)
	}
}

func preprocessRequest(ctx context.Context, quesmaRequest *quesma_api.Request, requestPreprocessors quesma_api.ProcessorChain) (*quesma_api.Request, context.Context, error) {
	var err error
	var processedRequest = quesmaRequest
	for _, preprocessor := range requestPreprocessors {
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

func (r *RouterV2) sendHttpRequestToElastic(ctx context.Context, req *http.Request,
	reqBody []byte, isManagement bool) chan elasticResultV2 {
	elkResponseChan := make(chan elasticResultV2)

	// If Quesma is exposing unauthenticated API but underlying Elasticsearch requires authentication, we should add the
	if r.Config.DisableAuth && req.Header.Get("Authorization") == "" && r.Config.Elasticsearch.User != "" {
		logger.DebugWithCtx(ctx).Msgf("path=%s routed to Elasticsearch, need add auth header to the request", req.URL)
		req.SetBasicAuth(r.Config.Elasticsearch.User, r.Config.Elasticsearch.Password)
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
		elkResponseChan <- recordRequestToElasticV2(req.URL.Path, r.debugInfoCollector, func() elasticResultV2 {

			isWrite := elasticsearch.IsWriteRequest(req)

			phoneHome := r.phoneHomeAgent

			var span diag.Span
			if isManagement {
				if isWrite {
					span = phoneHome.ElasticBypassedWriteRequestsDuration().Begin()
				} else {
					span = phoneHome.ElasticBypassedReadRequestsDuration().Begin()
				}
			} else {
				if isWrite {
					span = phoneHome.ElasticWriteRequestsDuration().Begin()
				} else {
					span = phoneHome.ElasticReadRequestsDuration().Begin()
				}
			}

			resp, err := r.sendHttpRequest(ctx, r.Config.Elasticsearch.Url.String(), req, reqBody)
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

func recordRequestToClickhouseV2(path string, qmc diag.DebugInfoCollector, requestFunc func() (*quesma_api.Result, error)) (*quesma_api.Result, error) {
	statName := ui.RequestStatisticKibana2Clickhouse
	if isIngestV2(path) {
		statName = ui.RequestStatisticIngest2Clickhouse
	}
	now := time.Now()
	response, err := requestFunc()
	if qmc != nil {
		qmc.RecordRequest(statName, time.Since(now), err != nil)
	}
	return response, err
}

func recordRequestToElasticV2(path string, qmc diag.DebugInfoCollector, requestFunc func() elasticResultV2) elasticResultV2 {
	statName := ui.RequestStatisticKibana2Elasticsearch
	if isIngestV2(path) {
		statName = ui.RequestStatisticIngest2Elasticsearch
	}
	now := time.Now()
	response := requestFunc()
	if qmc != nil {
		qmc.RecordRequest(statName, time.Since(now), !isResponseOkV2(response.response))
	}
	return response
}

func PeekBodyV2(r *http.Request) ([]byte, error) {
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
			if key != HttpHeaderContentLength {
				if w.Header().Get(key) == "" {
					w.Header().Add(key, value)
				}
			}
		}
	}
}

func (r *RouterV2) sendHttpRequest(ctx context.Context, address string, originalReq *http.Request, originalReqBody []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, originalReq.Method, address+originalReq.URL.String(), bytes.NewBuffer(originalReqBody))

	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error creating request: %v", err)
		return nil, err
	}

	req.Header = originalReq.Header

	resp, err := r.HttpClient.Do(req)
	if err != nil {
		logger.ErrorWithCtxAndReason(ctx, "No network connection").
			Msgf("Error sending request: %v", err)
		return nil, err
	}

	return resp, nil
}
