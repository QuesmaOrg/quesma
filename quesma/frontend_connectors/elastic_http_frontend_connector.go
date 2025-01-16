// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"net/http"
)

type ElasticHttpIngestFrontendConnector struct {
	*BasicHTTPFrontendConnector
}

func NewElasticHttpIngestFrontendConnector(endpoint string,
	logManager *clickhouse.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration, router quesma_api.Router) *ElasticHttpIngestFrontendConnector {

	fc := &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: NewBasicHTTPFrontendConnector(endpoint, config),
	}
	fallback := func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		fc.BasicHTTPFrontendConnector.GetRouterInstance().ElasticFallback(req.Decision, ctx, writer, req.OriginalRequest, []byte(req.Body), logManager, registry)
		return nil, nil
	}

	router.AddFallbackHandler(fallback)
	fc.AddRouter(router)

	return fc
}

type ElasticHttpQueryFrontendConnector struct {
	*BasicHTTPFrontendConnector
}

func NewElasticHttpQueryFrontendConnector(endpoint string,
	logManager *clickhouse.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration, router quesma_api.Router) *ElasticHttpIngestFrontendConnector {

	fc := &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: NewBasicHTTPFrontendConnector(endpoint, config),
	}
	fallback := func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		fc.BasicHTTPFrontendConnector.GetRouterInstance().ElasticFallback(req.Decision, ctx, writer, req.OriginalRequest, []byte(req.Body), logManager, registry)
		return nil, nil
	}
	router.AddFallbackHandler(fallback)
	fc.AddRouter(router)
	return fc
}
