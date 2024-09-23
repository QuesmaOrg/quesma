package quesma

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"quesma/logger"
	"quesma/quesma/config"
	"sync"
)

type authMiddleware struct {
	nextHttpHandler http.Handler
	authHeaderCache sync.Map
	esConf          config.ElasticsearchConfiguration
}

func NewAuthMiddleware(next http.Handler, esConf config.ElasticsearchConfiguration) http.Handler {
	return &authMiddleware{nextHttpHandler: next, esConf: esConf}
}

func (a *authMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if _, ok := a.authHeaderCache.Load(auth); ok {
		logger.Info().Msgf("PRZEMYSLAW AUTH FROM CACHE")
		a.nextHttpHandler.ServeHTTP(w, r)
		return
	}

	if authenticated := a.authenticateWithElasticsearch(auth); authenticated {
		a.authHeaderCache.Store(auth, struct{}{})
	} else {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	a.nextHttpHandler.ServeHTTP(w, r)
}

func (a *authMiddleware) authenticateWithElasticsearch(header string) bool {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/_security/_authenticate", a.esConf.Url), nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return false
	}
	req.Header.Add("Authorization", header)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}
