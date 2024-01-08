package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

const targetEnv = "ELASTIC_URL"

func main() {
	remote, err := url.Parse(os.Getenv(targetEnv))
	if err != nil {
		panic(err)
	}
	handler := func(p *httputil.ReverseProxy) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "POST" {
				go dualWrite(r)
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

func dualWrite(r *http.Request) {
	fmt.Printf("POST: %+v\n", r.URL.String())
	if strings.Contains(r.URL.String(), "/_bulk") {
		buf := new(bytes.Buffer)
		buf.ReadFrom(r.Body)
		for _, op := range strings.Fields(buf.String()) {
			fmt.Printf("  --> clickhouse, body: %s\n", op)
		}
	} else if strings.Contains(r.URL.String(), "/_doc") {
		buf := new(bytes.Buffer)
		buf.ReadFrom(r.Body)
		fmt.Printf("  --> clickhouse, body: %s\n", buf.String())
	} else {
		fmt.Printf("  --> pass-through\n")
	}
}
