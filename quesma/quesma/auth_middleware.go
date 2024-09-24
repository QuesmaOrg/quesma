// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"net/http"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/util"
	"sync"
	"time"
)

const cacheWipeInterval = 10 * time.Second

// authMiddleware a simple implementation of an authentication middleware,
// which checks the Authorization header and validates it against Elasticsearch.
//
// If the validation is positive, the Authorization header is stored in a cache to avoid unnecessary calls to Elasticsearch preceding each request.
// The cache is wiped every 10 minutes - all items at once, perhaps this could be revisited in the future.
type authMiddleware struct {
	nextHttpHandler   http.Handler
	authHeaderCache   sync.Map
	cacheWipeInterval time.Duration
	esClient          elasticsearch.SimpleClient
}

func NewAuthMiddleware(next http.Handler, esConf config.ElasticsearchConfiguration) http.Handler {
	esClient := elasticsearch.NewSimpleClient(&esConf)
	middleware := &authMiddleware{nextHttpHandler: next, esClient: *esClient, cacheWipeInterval: cacheWipeInterval}
	go middleware.startCacheWipeScheduler()
	return middleware
}

func (a *authMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var userName string
	if val, err := util.ExtractUsernameFromBasicAuthHeader(auth); err != nil {
		logger.Warn().Msgf("Failed to extract username from auth header: %v", err)
		userName = val
	}
	if _, ok := a.authHeaderCache.Load(auth); ok {
		logger.Info().Msgf("[AUTH] [%s] called by [%s] - credentials loaded from cache", r.URL, userName)
		a.nextHttpHandler.ServeHTTP(w, r)
		return
	}

	if authenticated := a.esClient.Authenticate(r.Context(), auth); authenticated {
		logger.DebugWithCtx(r.Context()).Msgf("[AUTH] [%s] called by [%s] - authenticated against Elasticsearch, storing in cache", r.URL, userName)
		a.authHeaderCache.Store(auth, struct{}{})
	} else {
		logger.DebugWithCtx(r.Context()).Msgf("[AUTH] [%s] called by [%s] - authentication against Elasticsearch failed", r.URL, userName)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	a.nextHttpHandler.ServeHTTP(w, r)
}

func (a *authMiddleware) startCacheWipeScheduler() {
	defer func() {
		if r := recover(); r != nil {
			logger.Error().Msgf("Recovered from panic during auth middleware cache wiping: [%v]", r)
		}
	}()
	ticker := time.NewTicker(a.cacheWipeInterval)
	defer ticker.Stop()
	for {
		<-ticker.C
		a.wipeCache()
	}
}

func (a *authMiddleware) wipeCache() {
	logger.Debug().Msgf("[AUTH] wiping auth header cache")
	a.authHeaderCache.Range(func(key, value interface{}) bool {
		a.authHeaderCache.Delete(key)
		return true
	})
}
