package quesma

import (
	"bytes"
	"io"
	"log"
	"mitmproxy/quesma/quesma/gzip"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
)

func NewResponseDecorator(tcpPort string, requestId int64, queryDebugger *QueryDebugger) *http.Server {
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

			// httputil.ReverseProxy does not serve
			// our purpose which is copy RequestID header from request to response.
			// The only thing http.ReverseProxy can do is to rewrite returned request OR response
			// therefore we have to pass id via closure
			proxy := httputil.NewSingleHostReverseProxy(remote)
			proxy.ModifyResponse = func(resp *http.Response) error {
				reader := resp.Body
				body, err := io.ReadAll(reader)
				if err != nil {
					log.Fatal(err)
				}
				resp.Body = io.NopCloser(bytes.NewBuffer(body))

				if strings.Contains(req.RequestURI, "/_search") || strings.Contains(req.RequestURI, "/_async_search") {
					isGzipped := strings.Contains(resp.Header.Get("Content-Encoding"), "gzip")
					if isGzipped {
						body, err = gzip.UnZip(body)
						if err != nil {
							log.Println("Error unzipping:", err)
							return err
						}
					}
					queryDebugger.PushPrimaryInfo(&QueryDebugPrimarySource{req.Header.Get("RequestId"), body})
				}
				return nil
			}
			proxy.ServeHTTP(writer, req)
		}),
	}
}
