package quesma

import (
	"bytes"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma/config"
	"net/http"
	"os"
	"strings"
)

const (
	HealthPath          = "/_cluster/health"
	BulkPath            = "/_bulk"
	CreateTablePath     = "/_createTable"
	InsertPath          = "/_insert"
	SearchPath          = "/_search"
	AsyncSearchPath     = "/logs-*-*/_async_search"
	ElasticInternalPath = "/_"
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

func configureRouting(config config.QuesmaConfiguration, lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc(HealthPath, ok)
	router.PathPrefix(CreateTablePath).HandlerFunc(createTable(lm))
	router.PathPrefix(InsertPath).HandlerFunc(processInsert(lm))
	router.PathPrefix(BulkPath).HandlerFunc(bulk(lm, rm, queryDebugger, config)).Methods("POST")
	router.PathPrefix(SearchPath).HandlerFunc(search(lm, rm, queryDebugger)).Methods("POST")
	router.PathPrefix(AsyncSearchPath).HandlerFunc(asyncSearch(lm, rm, queryDebugger)).Methods("POST")
	router.PathPrefix(ElasticInternalPath).HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("unrecognized internal path: %s\n", r.RequestURI)
	})
	router.PathPrefix("/.").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("internal index access '%s', ignoring...\n", strings.Split(r.RequestURI, "/")[1])
	})
	router.PathPrefix("/{index}/_doc").HandlerFunc(index(lm, rm, queryDebugger, config)).Methods("POST")
	router.PathPrefix("/{index}/_bulk").HandlerFunc(bulkVar(lm, rm, queryDebugger, config)).Methods("POST")
	router.PathPrefix("/{index}/_search").HandlerFunc(searchVar(lm, rm, queryDebugger)).Methods("POST")
	return router
}

func search(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("RequestId")
		go handleSearch("", body, lm, rm, queryDebugger, id)
	})
}

func asyncSearch(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("RequestId")
		go handleAsyncSearch("", body, lm, rm, queryDebugger, id)
	})
}

func searchVar(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("RequestId")
		vars := mux.Vars(r)
		go handleSearch(vars["index"], body, lm, rm, queryDebugger, id)
	})
}

func index(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger, config config.QuesmaConfiguration) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		go dualWrite(vars["index"], string(body), lm, config)
	})
}

func bulk(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger, config config.QuesmaConfiguration) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		go dualWriteBulk("", string(body), lm, config)
	})
}

func bulkVar(lm *clickhouse.LogManager, rm *ResponseMatcher, queryDebugger *QueryDebugger, config config.QuesmaConfiguration) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		go dualWriteBulk(vars["index"], string(body), lm, config)
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
