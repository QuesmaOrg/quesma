// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/schema"
	quesma_api "quesma_v2/core"
	"quesma_v2/core/diag"
	"strings"
	"sync"
)

type BasicHTTPFrontendConnector struct {
	listener        *http.Server
	router          quesma_api.Router
	mutex           sync.Mutex
	responseMutator func(w http.ResponseWriter) http.ResponseWriter
	endpoint        string
	routerInstance  *RouterV2
	logManager      *clickhouse.LogManager
	registry        schema.Registry
	config          *config.QuesmaConfiguration

	phoneHomeClient    diag.PhoneHomeClient
	debugInfoCollector diag.DebugInfoCollector
}

func (h *BasicHTTPFrontendConnector) GetChildComponents() []interface{} {
	components := make([]interface{}, 0)

	if h.router != nil {
		components = append(components, h.router)
	}

	if h.routerInstance != nil {
		components = append(components, h.routerInstance)
	}
	return components
}

func (h *BasicHTTPFrontendConnector) SetDependencies(deps quesma_api.Dependencies) {
	h.phoneHomeClient = deps.PhoneHomeAgent()
	h.debugInfoCollector = deps.DebugInfoCollector()
}

func NewBasicHTTPFrontendConnector(endpoint string, config *config.QuesmaConfiguration) *BasicHTTPFrontendConnector {

	return &BasicHTTPFrontendConnector{
		endpoint:       endpoint,
		config:         config,
		routerInstance: NewRouterV2(config),
		logManager:     nil,
		registry:       nil,
		responseMutator: func(w http.ResponseWriter) http.ResponseWriter {
			return w
		},
	}
}

func (h *BasicHTTPFrontendConnector) AddRouter(router quesma_api.Router) {
	h.router = router
}

func (h *BasicHTTPFrontendConnector) GetRouter() quesma_api.Router {
	return h.router
}

func (h *BasicHTTPFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer recovery.LogPanic()

	ctx := req.Context()
	requestPreprocessors := quesma_api.ProcessorChain{}
	requestPreprocessors = append(requestPreprocessors, quesma_api.NewTraceIdPreprocessor())

	reqBody, err := PeekBodyV2(req)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	ua := req.Header.Get("User-Agent")
	if h.phoneHomeClient != nil {
		h.phoneHomeClient.UserAgentCounters().Add(ua, 1)
	}

	quesmaRequest, ctx, err := preprocessRequest(ctx, &quesma_api.Request{
		Method:          req.Method,
		Path:            strings.TrimSuffix(req.URL.Path, "/"),
		Params:          map[string]string{},
		Headers:         req.Header,
		QueryParams:     req.URL.Query(),
		Body:            string(reqBody),
		OriginalRequest: req,
	}, requestPreprocessors)

	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error preprocessing request: %v", err)
	}

	quesmaRequest.ParsedBody = types.ParseRequestBody(quesmaRequest.Body)

	handlersPipe, decision := h.router.Matches(quesmaRequest)

	if decision != nil {
		w.Header().Set(QuesmaTableResolverHeader, decision.String())
	} else {
		w.Header().Set(QuesmaTableResolverHeader, "n/a")
	}
	dispatcher := &quesma_api.Dispatcher{}
	w = h.responseMutator(w)

	if handlersPipe != nil {
		quesmaResponse, err := recordRequestToClickhouseV2(req.URL.Path, h.debugInfoCollector, func() (*quesma_api.Result, error) {
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
			h.routerInstance.errorResponseV2(ctx, err, w)
		}
	} else {
		if h.router.GetFallbackHandler() != nil {
			fmt.Printf("No handler found for path: %s\n", req.URL.Path)
			handler := h.router.GetFallbackHandler()
			result, _ := handler(context.Background(), &quesma_api.Request{OriginalRequest: req})
			_, err := w.Write(result.GenericResult.([]byte))
			if err != nil {
				fmt.Printf("Error writing response: %s\n", err)
			}
		}
	}
}

func (h *BasicHTTPFrontendConnector) Listen() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.listener != nil {
		// TODO handle this gracefully and return correct error
		return nil
	}
	h.listener = &http.Server{}
	h.listener.Addr = h.endpoint
	h.listener.Handler = h
	go func() {
		err := h.listener.ListenAndServe()
		// TODO: Handle error
		_ = err
	}()

	return nil
}

func (h *BasicHTTPFrontendConnector) Stop(ctx context.Context) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.listener == nil {
		return nil
	}
	err := h.listener.Shutdown(ctx)
	if err != nil {
		return err
	}
	return h.listener.Close()
}

func (h *BasicHTTPFrontendConnector) GetEndpoint() string {
	return h.endpoint
}

func ReadRequestBody(request *http.Request) ([]byte, error) {
	reqBody, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	request.Body = io.NopCloser(bytes.NewBuffer(reqBody))
	return reqBody, nil
}
