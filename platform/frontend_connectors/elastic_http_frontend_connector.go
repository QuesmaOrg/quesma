// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/schema"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"net/http"
)

type ElasticHttpIngestFrontendConnector struct {
	*BasicHTTPFrontendConnector
}

func NewElasticHttpIngestFrontendConnector(endpoint string,
	logManager *database_common.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration, router quesma_api.Router) *ElasticHttpIngestFrontendConnector {

	fc := &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: NewBasicHTTPFrontendConnector(endpoint, config),
	}
	fallback := func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		fc.BasicHTTPFrontendConnector.GetDispatcherInstance().ElasticFallback(req.Decision, ctx, writer, req.OriginalRequest, []byte(req.Body), logManager, registry)
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
	logManager *database_common.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration, router quesma_api.Router) *ElasticHttpIngestFrontendConnector {

	fc := &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: NewBasicHTTPFrontendConnector(endpoint, config),
	}
	fallback := func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		fc.BasicHTTPFrontendConnector.GetDispatcherInstance().ElasticFallback(req.Decision, ctx, writer, req.OriginalRequest, []byte(req.Body), logManager, registry)
		return nil, nil
	}
	router.AddFallbackHandler(fallback)
	fc.AddRouter(router)
	return fc
}
