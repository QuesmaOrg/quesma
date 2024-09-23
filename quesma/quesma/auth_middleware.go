package quesma

import (
	"net/http"
	"quesma/elasticsearch"
	"quesma/quesma/config"
	"sync"
)

type authMiddleware struct {
	nextHttpHandler http.Handler
	authHeaderCache sync.Map
	esConf          config.ElasticsearchConfiguration
	esClient        elasticsearch.SimpleClient
}

func NewAuthMiddleware(next http.Handler, esConf config.ElasticsearchConfiguration) http.Handler {
	esClient := elasticsearch.NewSimpleClient(&esConf)
	return &authMiddleware{nextHttpHandler: next, esClient: *esClient}
}

func (a *authMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if _, ok := a.authHeaderCache.Load(auth); ok {
		a.nextHttpHandler.ServeHTTP(w, r)
		return
	}

	if authenticated := a.esClient.Authenticate(auth); authenticated {
		a.authHeaderCache.Store(auth, struct{}{})
	} else {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	a.nextHttpHandler.ServeHTTP(w, r)
}
