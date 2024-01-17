package quesma

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
)

func unzip(gzippedData []byte) ([]byte, error) {
	// Create a reader for the gzipped data
	reader := bytes.NewReader(gzippedData)

	// Create a gzip reader
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()

	// Read the unzipped data
	unzippedData, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, err
	}

	return unzippedData, nil
}

func NewResponseDecorator(tcpPort string, requestId int64, matcher *ResponseMatcher) *http.Server {
	remote, err := url.Parse(REMOTE_URL)
	if err != nil {
		log.Fatal("Cannot parse target url:", err)
	}
	return &http.Server{
		Addr: ":" + tcpPort,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
			req.Host = remote.Host
			req.URL.Host = remote.Host
			req.URL.Scheme = "http"
			log.Println(req.URL.Host)

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

				if strings.Contains(req.RequestURI, "/_search?pretty") {
					isGzipped := strings.Contains(resp.Header.Get("Content-Encoding"), "gzip")
					if isGzipped {
						unzippedBuffer, err := unzip(body)
						if err != nil {
							log.Println("Error unzipping:", err)
							return err
						}
						matcher.Push(&QResponse{req.Header.Get("RequestId"), unzippedBuffer})
					} else {
						matcher.Push(&QResponse{req.Header.Get("RequestId"), body})
					}
				}
				return nil
			}
			proxy.ServeHTTP(writer, req)
		}),
	}
}
