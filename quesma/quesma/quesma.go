package quesma

import (
	"bytes"
	"context"
	"io"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/proxy"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/gzip"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/routes"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	httpHeaderContentLength = "Content-Length"
)
const (
	quesmaSourceHeader     = "X-Quesma-Source"
	quesmaSourceElastic    = "Elasticsearch"
	quesmaSourceClickhouse = "Clickhouse"
)

type (
	Quesma struct {
		processor               requestProcessor
		publicTcpPort           network.Port
		quesmaManagementConsole *ui.QuesmaManagementConsole
		config                  config.QuesmaConfiguration
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
	logUnexpected(elkResponse.Header, w.Header(), id)
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
}

func sendElkResponseToQuesmaConsole(ctx context.Context, elkResponse *http.Response, console *ui.QuesmaManagementConsole) {
	reader := elkResponse.Body
	body, err := io.ReadAll(reader)
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error reading response body: %v", err)
		return
	}
	elkResponse.Body = io.NopCloser(bytes.NewBuffer(body))

	if gzip.IsGzipped(elkResponse) {
		body, err = gzip.UnZip(body)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("Error unzipping: %v", err)
		}
	}
	console.PushPrimaryInfo(&ui.QueryDebugPrimarySource{Id: id, QueryResp: body})
}

func NewQuesmaTcpProxy(config config.QuesmaConfiguration, logChan <-chan string, inspect bool) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logChan)
	return &Quesma{
		processor:               proxy.NewTcpProxy(config.PublicTcpPort, config.ElasticsearchUrl.Host, inspect),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func NewHttpProxy(logManager *clickhouse.LogManager, config config.QuesmaConfiguration, logChan <-chan string) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logChan)
	router := configureRouter(config, logManager, quesmaManagementConsole)
	return &Quesma{
		processor:               newDualWriteProxy(logManager, config, router, quesmaManagementConsole),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func logUnexpected(elasticHeader, quesmaHeader http.Header, id string) {
	for headerKey := range elasticHeader {
		if _, ok := quesmaHeader[headerKey]; !ok {
			logger.Warn().Str(logger.RID, id).Msgf("Header %s is missing in Quesma's response", headerKey)
		}
	}
}

func reroute(ctx context.Context, w http.ResponseWriter, req *http.Request, reqBody []byte, router *mux.PathRouter, config config.QuesmaConfiguration, quesmaManagementConsole *ui.QuesmaManagementConsole) {
	if router.Matches(req.URL.Path, req.Method) {
		elkResponseChan := sendHttpRequestToElastic(ctx, config, quesmaManagementConsole, req, reqBody)
		quesmaResponse, err := recordRequestToClickhouse(req.URL.Path, quesmaManagementConsole, func() (*mux.Result, error) {
			return router.Execute(ctx, req.URL.Path, string(reqBody), req.Method)
		})
		elkResponse := <-elkResponseChan
		sendElkResponseToQuesmaConsole(ctx, elkResponse, quesmaManagementConsole)

		if err == nil {
			logger.Debug().Ctx(ctx).Msg("responding from quesma")
			unzipped := []byte{}
			if quesmaResponse != nil {
				unzipped = []byte(quesmaResponse.Body)
			}
			if string(unzipped) != "" {
				for key, value := range quesmaResponse.Meta {
					w.Header().Set(key, value)
				}
				zip := strings.Contains(req.Header.Get("Accept-Encoding"), "gzip")
				if zip {
					w.Header().Set("Content-Encoding", "gzip")
				}
				w.Header().Set(quesmaSourceHeader, quesmaSourceClickhouse)
				w.WriteHeader(quesmaResponse.StatusCode)
				responseFromQuesma(ctx, unzipped, w, elkResponse, zip)
			} else {
				copyHeaders(w, elkResponse)
				w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
				w.WriteHeader(elkResponse.StatusCode)
				responseFromElastic(ctx, elkResponse, w)
			}
		} else {
			logger.Error().Ctx(ctx).Msgf("Error processing request: %v, responding from Elastic", err)
			w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
			w.WriteHeader(elkResponse.StatusCode)
			responseFromElastic(ctx, elkResponse, w)
		}
	} else {
		response := recordRequestToElastic(req.URL.Path, quesmaManagementConsole, func() *http.Response {
			return sendHttpRequest(ctx, config.ElasticsearchUrl.String(), req, reqBody)
		})
		copyHeaders(w, response)
		w.Header().Set(quesmaSourceHeader, quesmaSourceElastic)
		w.WriteHeader(response.StatusCode)
		responseFromElastic(ctx, response, w)
	}
}

func sendHttpRequestToElastic(ctx context.Context, config config.QuesmaConfiguration, qmc *ui.QuesmaManagementConsole,
	req *http.Request, reqBody []byte) chan *http.Response {
	elkResponseChan := make(chan *http.Response)
	go func() {
		elkResponseChan <- recordRequestToElastic(req.URL.Path, qmc, func() *http.Response {
			return sendHttpRequest(ctx, config.ElasticsearchUrl.String(), req, reqBody)
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

func recordRequestToElastic(path string, qmc *ui.QuesmaManagementConsole, requestFunc func() *http.Response) *http.Response {
	statName := ui.RequestStatisticKibana2Elasticsearch
	if isIngest(path) {
		statName = ui.RequestStatisticIngest2Elasticsearch
	}
	now := time.Now()
	response := requestFunc()
	qmc.RecordRequest(statName, time.Since(now), !isResponseOk(response))
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
	return context.WithValue(r.Context(), tracing.RequestIdCtxKey, rid)
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

func sendHttpRequest(ctx context.Context, address string, originalReq *http.Request, originalReqBody []byte) *http.Response {
	req, err := http.NewRequestWithContext(ctx, originalReq.Method, address+originalReq.URL.String(), bytes.NewBuffer(originalReqBody))
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error creating request: %v", err)
		return nil
	}

	req.Header = originalReq.Header
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		logger.ErrorWithCtxAndReason(ctx, "No network connection").
			Msgf("Error sending request: %v", err)
		return nil
	}

	return resp
}
