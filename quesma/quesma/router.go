package quesma

import (
	"bytes"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/gzip"
	"net/http"
	"os"
	"strings"
)

const (
	HealthPath          = "/_cluster/health"
	BulkPath            = "/_bulk"
	CreateTablePath     = "/_createTable"
	InsertPath          = "/_insert"
	NodesPath           = "/_nodes"
	SearchPath          = "/_search"
	AsyncSearchPath     = "/_async_search"
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

func configureRouting(config config.QuesmaConfiguration, lm *clickhouse.LogManager, queryDebugger *QueryDebugger) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc(HealthPath, ok)
	router.PathPrefix(CreateTablePath).HandlerFunc(createTable(lm))
	router.PathPrefix(InsertPath).HandlerFunc(processInsert(lm))
	router.PathPrefix(NodesPath).HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		body := []byte(`{"nodes": {
         "UTIfdE4zTLOc_QrCusVWeQ": {
             "http": {
                 "publish_address": "mitmproxy:9200"
             },
             "ip": "mitmproxy",
             "version": "8.11.1"
         }
     }
 }`)

		zipped, _ := gzip.Zip(body)
		_, _ = w.Write(zipped)
	})
	router.PathPrefix(BulkPath).HandlerFunc(bulk(lm, queryDebugger, config)).Methods("POST")
	router.PathPrefix(SearchPath).HandlerFunc(search(lm, queryDebugger)).Methods("POST")
	router.PathPrefix(AsyncSearchPath).HandlerFunc(asyncSearch(lm, queryDebugger)).Methods("POST")
	router.PathPrefix(ElasticInternalPath).HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("unrecognized internal path: %s\n", r.RequestURI)
	})
	router.PathPrefix("/.").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("internal index access '%s', ignoring...\n", strings.Split(r.RequestURI, "/")[1])
	})
	router.PathPrefix("/{index}/_doc").HandlerFunc(index(lm, queryDebugger, config)).Methods("POST")
	router.PathPrefix("/{index}/_bulk").HandlerFunc(bulkVar(lm, queryDebugger, config)).Methods("POST")
	router.PathPrefix("/{index}/_search").HandlerFunc(searchVar(lm, queryDebugger)).Methods("POST")
	return router
}

func search(lm *clickhouse.LogManager, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("RequestId")
		go handleSearch("", body, lm, queryDebugger, id)
	})
}

func asyncSearch(lm *clickhouse.LogManager, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("RequestId")
		go handleAsyncSearch("", body, lm, queryDebugger, id)
	})
}

func searchVar(lm *clickhouse.LogManager, queryDebugger *QueryDebugger) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("RequestId")
		vars := mux.Vars(r)
		go handleSearch(vars["index"], body, lm, queryDebugger, id)
	})
}

func index(lm *clickhouse.LogManager, queryDebugger *QueryDebugger, config config.QuesmaConfiguration) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		go dualWrite(vars["index"], string(body), lm, config)
	})
}

func bulk(lm *clickhouse.LogManager, queryDebugger *QueryDebugger, config config.QuesmaConfiguration) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		go dualWriteBulk("", string(body), lm, config)
	})
}

func bulkVar(lm *clickhouse.LogManager, queryDebugger *QueryDebugger, config config.QuesmaConfiguration) func(http.ResponseWriter, *http.Request) {
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
