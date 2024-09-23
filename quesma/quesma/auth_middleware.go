package quesma

import (
	"net/http"
	"quesma/elasticsearch"
	"quesma/quesma/config"
	"sync"
	"time"
)

type authMiddleware struct {
	nextHttpHandler   http.Handler
	authHeaderCache   sync.Map
	cacheWipeInterval time.Duration
	esClient          elasticsearch.SimpleClient
}

func NewAuthMiddleware(next http.Handler, esConf config.ElasticsearchConfiguration) http.Handler {
	esClient := elasticsearch.NewSimpleClient(&esConf)
	middleware := &authMiddleware{nextHttpHandler: next, esClient: *esClient, cacheWipeInterval: 10 * time.Minute}
	go middleware.startCacheWipeScheduler()
	return middleware
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

func (a *authMiddleware) startCacheWipeScheduler() {
	ticker := time.NewTicker(a.cacheWipeInterval)
	defer ticker.Stop()
	for {
		<-ticker.C
		a.wipeCache()
	}
}

func (a *authMiddleware) wipeCache() {
	a.authHeaderCache.Range(func(key, value interface{}) bool {
		a.authHeaderCache.Delete(key)
		return true
	})
}
