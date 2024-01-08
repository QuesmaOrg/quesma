package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

const targetEnv = "ELASTIC_URL"

var lm = clickhouse.NewLogManager()

func main() {
	tm := clickhouse.NewTableManager()
	tm.Migrate()

	remote, err := url.Parse(os.Getenv(targetEnv))
	if err != nil {
		panic(err)
	}
	handler := func(p *httputil.ReverseProxy) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "POST" {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					log.Fatal(err)
				}
				r.Body = io.NopCloser(bytes.NewBuffer(body))
				go dualWrite(r.RequestURI, string(body))
			}
			r.Host = remote.Host
			p.ServeHTTP(w, r)
		}
	}
	proxy := httputil.NewSingleHostReverseProxy(remote)
	http.HandleFunc("/", handler(proxy))
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(err)
	}
}

func dualWrite(url string, body string) {
	if strings.Contains(url, "/_bulk") {
		fmt.Printf("%s --> clickhouse\n", url)
		for _, op := range strings.Fields(body) {
			fmt.Printf("  --> clickhouse, body: %s\n", op)
		}
	} else if strings.Contains(url, "/logs-generic-default/_doc") {
		lm.Insert(body)
		fmt.Printf("%s --> clickhouse, body: %s\n", url, body)
	} else {
		fmt.Printf("%s --> pass-through\n", url)
	}
}
