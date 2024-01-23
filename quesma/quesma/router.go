package quesma

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
)

const (
	QuesmaInternalPath = "/_quesma"
	HealthPath         = QuesmaInternalPath + "/health"
	BulkPath           = "/_bulk"
	CreateTablePath    = "/_createTable"
	InsertPath         = "/_insert"
	SearchPath         = "/_search"
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
	router.PathPrefix(BulkPath).HandlerFunc(bulk(lm, rm, queryDebugger)).Methods("POST")
	router.PathPrefix(SearchPath).HandlerFunc(search(lm, rm, queryDebugger)).Methods("POST")
	router.PathPrefix(InternalPath).HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("unrecognized internal path: %s\n", r.RequestURI)
	})
	router.PathPrefix("/.").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("internal index access '%s', ignoring...\n", strings.Split(r.RequestURI, "/")[1])
	})
	router.PathPrefix("/{index}/_doc").HandlerFunc(index(lm, rm, queryDebugger)).Methods("POST")
	router.PathPrefix("/{index}/_bulk").HandlerFunc(bulkVar(lm, rm, queryDebugger)).Methods("POST")
	router.PathPrefix("/{index}/_search").HandlerFunc(searchVar(lm, rm, queryDebugger)).Methods("POST")
	return router
}

func ok(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(200)
}

func search(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("RequestId")
		go handleSearch("", body, lm, rm, queryDebugger, id)
	})
}

func searchVar(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("RequestId")
		vars := mux.Vars(r)
		go handleSearch(vars["index"], body, lm, rm, queryDebugger, id)
	})
}

func index(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		go dualWrite(vars["index"], string(body), lm)
	})
}

func bulk(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		go dualWriteBulk("", string(body), lm)
	})
}

func bulkVar(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		go dualWriteBulk(vars["index"], string(body), lm)
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
