package quesma

import (
	"bytes"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"net/http"
	"os"
	"strings"
)

const (
	QuesmaInternalPath = "/_quesma"
	HealthPath         = QuesmaInternalPath + "/health"
	BulkPath           = "/_bulk"
	CreateTablePath    = "/_createTable"
	InsertPath         = "/_insert"
	InternalPath       = "/_"
)

func bodyHandler(h func(body []byte, writer http.ResponseWriter, r *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		r.Body = io.NopCloser(bytes.NewBuffer(body))
		h(body, writer, r)
	}
}

func configureRouting(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc(HealthPath, ok)
	router.PathPrefix(CreateTablePath).HandlerFunc(createTable(lm))
	router.PathPrefix(InsertPath).HandlerFunc(processInsert(lm))
	router.PathPrefix(BulkPath).HandlerFunc(bulk(lm, rm, queryDebugger))
	router.PathPrefix(InternalPath).HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("unrecognized internal path: %s\n", r.RequestURI)
	})
	router.PathPrefix("/.").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("internal index access '%s', ignoring...\n", strings.Split(r.RequestURI, "/")[1])
	})
	router.PathPrefix("/{index}/_doc").HandlerFunc(index(lm, rm, queryDebugger))
	return router
}

func ok(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(200)
}

func index(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			go dualWrite(r.RequestURI, string(body), lm)
			id := r.Header.Get("RequestId")
			go handleQuery(r.RequestURI, body, lm, rm, queryDebugger, id)
		}
	})
}

func bulk(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		go dualWriteBulk(r.RequestURI, string(body), lm)
		id := r.Header.Get("RequestId")
		go handleQuery(r.RequestURI, body, lm, rm, queryDebugger, id)
	})
}

func processInsert(lm *clickhouse.LogManager) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		err := lm.ProcessInsertQuery("signoz_logs", string(body))
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
	})
}

func createTable(lm *clickhouse.LogManager) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("%s --> create table\n", r.RequestURI)
		_ = lm.ProcessCreateTableQuery(string(body), clickhouse.NewDefaultCHConfig())
	})
}
