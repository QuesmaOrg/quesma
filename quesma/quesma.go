package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const targetEnv = "ELASTIC_URL"

type Quesma struct {
	proxy      *httputil.ReverseProxy
	logManager *clickhouse.LogManager
	remoteUrl  *url.URL
}

func (q *Quesma) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		r.Body = io.NopCloser(bytes.NewBuffer(body))
		go dualWrite(r.RequestURI, string(body), q.logManager)
	}
	r.Host = q.remoteUrl.Host
	q.proxy.ServeHTTP(w, r)
}

func main() {
	lm := clickhouse.NewLogManager()
	defer lm.Close()
	tm := clickhouse.NewTableManager()
	tm.Migrate()

	remote, err := url.Parse(os.Getenv(targetEnv))
	if err != nil {
		panic(err)
	}
	quesma := Quesma{
		proxy:      httputil.NewSingleHostReverseProxy(remote),
		logManager: lm,
		remoteUrl:  remote,
	}
	server := http.Server{
		Addr:    ":8080",
		Handler: &quesma,
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("Error starting server:", err)
		}
	}()

	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Error during server shutdown:", err)
	}
}

func dualWrite(url string, body string, lm *clickhouse.LogManager) {
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
