package quesma

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync/atomic"
)

func NewResponseDecorator(tcpPort string, requestId int64, quesmaManagementConsole *QuesmaManagementConsole) *http.Server {
	remote, err := url.Parse(RemoteUrl)
	if err != nil {
		log.Fatal("Cannot parse target url:", err)
	}
	return &http.Server{
		Addr: ":" + tcpPort,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
			req.Host = remote.Host
			req.URL.Host = remote.Host
			req.URL.Scheme = "http"

			id := atomic.AddInt64(&requestId, 1)
			req.Header.Add("RequestId", strconv.FormatInt(id, 10))

			proxy := httputil.NewSingleHostReverseProxy(remote)
			proxy.ServeHTTP(writer, req)
		}),
	}
}
