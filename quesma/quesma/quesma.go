package quesma

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/feature"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/proxy"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/gzip"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/routes"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/tracing"
	"net/http"
	"strings"
	"time"
)

type (
	Quesma struct {
		processor               requestProcessor
		publicTcpPort           network.Port
		quesmaManagementConsole *ui.QuesmaManagementConsole
		config                  config.QuesmaConfiguration
		telemetryAgent          telemetry.PhoneHomeAgent
	}
	requestProcessor interface {
		Ingest()
		Stop(ctx context.Context)
	}
)

func responseFromElastic(ctx context.Context, elkResponse *http.Response, w http.ResponseWriter) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Elasticsearch")
	if _, err := io.Copy(w, elkResponse.Body); err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error copying response body: %v", err)
		http.Error(w, "Error copying response body", http.StatusInternalServerError)
		return
	}
	elkResponse.Body.Close()
}

func responseFromQuesma(ctx context.Context, unzipped []byte, w http.ResponseWriter, elkResponse *http.Response, zip bool) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Quesma")
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

func NewHttpProxy(phoneHomeAgent telemetry.PhoneHomeAgent, logManager *clickhouse.LogManager, indexManager elasticsearch.IndexManagement, config config.QuesmaConfiguration, logChan <-chan tracing.LogWithLevel) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logManager, indexManager, logChan, phoneHomeAgent)
	queryRunner := NewQueryRunner(logManager, config, indexManager, quesmaManagementConsole)
	router := configureRouter(config, logManager, quesmaManagementConsole, phoneHomeAgent, queryRunner)
	return &Quesma{
		telemetryAgent:          phoneHomeAgent,
		processor:               newDualWriteProxy(logManager, indexManager, config, router, quesmaManagementConsole, phoneHomeAgent, queryRunner),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

type router struct {
	config                  config.QuesmaConfiguration
	quesmaManagementConsole *ui.QuesmaManagementConsole
	phoneHomeAgent          telemetry.PhoneHomeAgent
	httpClient              *http.Client
}

func (r *router) reroute(ctx context.Context, w http.ResponseWriter, req *http.Request, reqBody []byte, router *mux.PathRouter, logManager *clickhouse.LogManager) {
	defer recovery.LogPanicWithCtx(ctx)
	if router.Matches(req.URL.Path, req.Method, string(reqBody)) {
		var elkResponseChan = make(chan elasticResult)

		if r.config.Elasticsearch.Call {
			elkResponseChan = r.sendHttpRequestToElastic(ctx, req, reqBody, false)
		}

		quesmaResponse, err := recordRequestToClickhouse(req.URL.Path, r.quesmaManagementConsole, func() (*mux.Result, error) {
			return router.Execute(ctx, req.URL.Path, string(reqBody), req.Method, req.Header)
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
			for key, value := range quesmaResponse.Meta {
				w.Header().Set(key, value)
			}
			if zip {
				w.Header().Set("Content-Encoding", "gzip")
			}
			w.Header().Set(quesmaSourceHeader, quesmaSourceClickhouse)
			w.WriteHeader(quesmaResponse.StatusCode)
			responseFromQuesma(ctx, unzipped, w, elkResponse, zip)

		} else {
			if elkResponse != nil && r.config.Mode == config.DualWriteQueryClickhouseFallback {
				logger.ErrorWithCtx(ctx).Msgf("Error processing request while responding from Elastic: %v", err)
				copyHeaders(w, elkResponse)
				w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
				w.WriteHeader(elkResponse.StatusCode)
				responseFromElastic(ctx, elkResponse, w)

			} else {
				logger.ErrorWithCtx(ctx).Msgf("Error processing request while responding from Quesma: %v", err)
				w.Header().Set(quesmaSourceHeader, quesmaSourceClickhouse)
				w.WriteHeader(500)

				requestId := "n/a"
				if contextRid, ok := ctx.Value(tracing.RequestIdCtxKey).(string); ok {
					requestId = contextRid
				}

				// We should not send our error message to the client. There can be sensitive information in it.
				// We will send ID of failed request instead
				responseFromQuesma(ctx, []byte(fmt.Sprintf("Internal server error. Request ID: %s\n", requestId)), w, elkResponse, zip)
			}
		}
	} else {

		feature.AnalyzeUnsupportedCalls(ctx, req.Method, req.URL.Path, logManager.ResolveIndexes)

		elkResponseChan := r.sendHttpRequestToElastic(ctx, req, reqBody, true)
		rawResponse := <-elkResponseChan
		response := rawResponse.response
		w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
		if response != nil {
			copyHeaders(w, response)
			w.WriteHeader(response.StatusCode)
			responseFromElastic(ctx, response, w)
		} else {
			w.WriteHeader(500)
			if rawResponse.error != nil {
				w.Write([]byte(rawResponse.error.Error()))
			}
		}
	}
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

			isWrite := false

			// Elastic API is not regular, and it is hard to determine if the request is read or write.
			// We would like to keep this separate from the router configuration.
			switch req.Method {
			case http.MethodPost:
				if strings.Contains(req.URL.Path, "/_bulk") ||
					strings.Contains(req.URL.Path, "/_doc") ||
					strings.Contains(req.URL.Path, "/_create") {
					isWrite = true
				}
				// other are read
			case http.MethodPut:
				isWrite = true
			case http.MethodDelete:
				isWrite = true
			default:
				isWrite = false
			}

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

func withTracing(r *http.Request) context.Context {
	rid := tracing.GetRequestId()
	r.Header.Add("RequestId", rid)
	ctx := r.Context()
	ctx = context.WithValue(ctx, tracing.RequestIdCtxKey, rid)
	ctx = context.WithValue(ctx, tracing.RequestPath, r.URL.Path)

	return ctx
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
