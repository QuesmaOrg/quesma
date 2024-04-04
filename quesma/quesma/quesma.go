package quesma

import (
	"bytes"
	"context"
	"io"
	"mitmproxy/quesma/clickhouse"
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
	"strconv"
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
		w.Header().Add(httpHeaderContentLength, strconv.Itoa(len(zipped)))
		_, _ = io.Copy(w, bytes.NewBuffer(zipped))
	} else {
		w.Header().Add(httpHeaderContentLength, strconv.Itoa(len(unzipped)))
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

func NewQuesmaTcpProxy(phoneHomeAgent telemetry.PhoneHomeAgent, config config.QuesmaConfiguration, logChan <-chan string, inspect bool) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, nil, logChan, phoneHomeAgent)
	return &Quesma{
		processor:               proxy.NewTcpProxy(config.PublicTcpPort, config.ElasticsearchUrl.Host, inspect),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func NewHttpProxy(phoneHomeAgent telemetry.PhoneHomeAgent, logManager *clickhouse.LogManager, config config.QuesmaConfiguration, logChan <-chan string) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logManager, logChan, phoneHomeAgent)
	queryRunner := NewQueryRunner()
	router := configureRouter(config, logManager, quesmaManagementConsole, phoneHomeAgent, queryRunner)
	return &Quesma{
		telemetryAgent:          phoneHomeAgent,
		processor:               newDualWriteProxy(logManager, config, router, quesmaManagementConsole, phoneHomeAgent, queryRunner),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

type rerouteParams struct {
	config                  config.QuesmaConfiguration
	quesmaManagementConsole *ui.QuesmaManagementConsole
	phoneHomeAgent          telemetry.PhoneHomeAgent
	httpClient              *http.Client
}

func (obj *rerouteParams) reroute(ctx context.Context, w http.ResponseWriter, req *http.Request, reqBody []byte, router *mux.PathRouter, logManager *clickhouse.LogManager) {
	if router.Matches(req.URL.Path, req.Method, string(reqBody)) {
		elkResponseChan := obj.sendHttpRequestToElastic(ctx, req, reqBody)
		quesmaResponse, err := recordRequestToClickhouse(req.URL.Path, obj.quesmaManagementConsole, func() (*mux.Result, error) {
			return router.Execute(ctx, req.URL.Path, string(reqBody), req.Method)
		})
		elkRawResponse := <-elkResponseChan
		elkResponse := elkRawResponse.response
		if elkResponse != nil {
			if routes.IsQueryPath(req.URL.Path) { // We should send only responses for search queries to Quesma console
				sendElkResponseToQuesmaConsole(ctx, elkRawResponse, obj.quesmaManagementConsole)
			}
			if !(elkResponse.StatusCode >= 200 && elkResponse.StatusCode < 300) {
				logger.Warn().Msgf("Elasticsearch returned unexpected status code [%d] when calling [%s %s]", elkResponse.StatusCode, req.Method, req.URL.Path)
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
				logger.Warn().Ctx(ctx).Str("url", req.URL.Path).Msg("Empty response from Clickhouse")
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
			if elkResponse != nil && obj.config.Mode == config.DualWriteQueryClickhouseFallback {
				logger.Error().Ctx(ctx).Msgf("Error processing request: %v, responding from Elastic", err)
				copyHeaders(w, elkResponse)
				w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
				w.WriteHeader(elkResponse.StatusCode)
				responseFromElastic(ctx, elkResponse, w)

			} else {
				logger.Error().Ctx(ctx).Msgf("Error processing request: %v, responding from Quesma", err)
				w.Header().Set(quesmaSourceHeader, quesmaSourceClickhouse)
				w.WriteHeader(500)
				responseFromQuesma(ctx, []byte(err.Error()), w, elkResponse, zip)
			}
		}
	} else {

		feature.AnalyzeUnsupportedCalls(ctx, req.Method, req.URL.Path, logManager.ResolveIndexes)

		elkResponseChan := obj.sendHttpRequestToElastic(ctx, req, reqBody)
		rawResponse := <-elkResponseChan
		response := rawResponse.response
		if response != nil {
			copyHeaders(w, response)
			w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
			w.WriteHeader(response.StatusCode)
			responseFromElastic(ctx, response, w)
		}
	}
}

type elasticResult struct {
	response *http.Response
	took     time.Duration
}

func (obj *rerouteParams) sendHttpRequestToElastic(ctx context.Context, req *http.Request,
	reqBody []byte) chan elasticResult {
	elkResponseChan := make(chan elasticResult)

	// If the request is authenticated, we should not override it with the configured user
	if req.Header.Get("Authorization") == "" && obj.config.ElasticsearchUser != "" {
		req.SetBasicAuth(obj.config.ElasticsearchUser, obj.config.ElasticsearchPassword)
	}

	go func() {
		elkResponseChan <- recordRequestToElastic(req.URL.Path, obj.quesmaManagementConsole, func() elasticResult {
			span := obj.phoneHomeAgent.ElasticQueryDuration().Begin()
			resp, err := obj.sendHttpRequest(ctx, obj.config.ElasticsearchUrl.String(), req, reqBody)
			took := span.End(err)
			return elasticResult{resp, took}
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

func (obj *rerouteParams) sendHttpRequest(ctx context.Context, address string, originalReq *http.Request, originalReqBody []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, originalReq.Method, address+originalReq.URL.String(), bytes.NewBuffer(originalReqBody))

	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error creating request: %v", err)
		return nil, err
	}

	req.Header = originalReq.Header

	resp, err := obj.httpClient.Do(req)
	if err != nil {
		logger.ErrorWithCtxAndReason(ctx, "No network connection").
			Msgf("Error sending request: %v", err)
		return nil, err
	}

	return resp, nil
}
