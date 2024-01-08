package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
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
				body, err := io.ReadAll(r.Body)
				if err != nil {
					log.Fatal(err)
				}
				r.Body = io.NopCloser(bytes.NewBuffer(body))
				go dualWrite(string(body))
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

func dualWrite(r string) {
	fmt.Printf("POST: %+v\n", r)
	if strings.Contains(r, "/_bulk") {
		fmt.Printf("  --> write to clickhouse,body: %+v\n", r)
		for _, op := range strings.Fields(r) {
			fmt.Printf("  --> clickhouse, body: %s\n", op)
		}
	} else if strings.Contains(r, "/_doc") {
		fmt.Printf("  --> clickhouse, body: %s\n", r)
	} else {
		fmt.Printf("  --> pass-through\n")
	}
}
