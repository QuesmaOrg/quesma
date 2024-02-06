package quesma

import (
	"bytes"
	"context"
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
		ctx := context.WithValue(r.Context(), RequestId{}, r.Header.Get("RequestId"))
		r = r.WithContext(ctx)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		r.Body = io.NopCloser(bytes.NewBuffer(body))
		h(body, writer, r)
	}
}

func configureRouting(config config.QuesmaConfiguration, lm *clickhouse.LogManager, quesmaManagementConsole *QuesmaManagementConsole) *mux.Router {
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
	router.PathPrefix(BulkPath).HandlerFunc(bulk(lm, config)).Methods("POST")
	router.PathPrefix(SearchPath).HandlerFunc(search(lm, quesmaManagementConsole)).Methods("POST")
	router.PathPrefix("/{index}" + AsyncSearchPath).HandlerFunc(asyncSearch(lm, quesmaManagementConsole)).Methods("POST")
	router.PathPrefix(ElasticInternalPath).HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("unrecognized internal path: %s\n", r.RequestURI)
	})
	router.PathPrefix("/.").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		fmt.Printf("internal index access '%s', ignoring...\n", strings.Split(r.RequestURI, "/")[1])
	})
	router.PathPrefix("/{index}/_doc").HandlerFunc(index(lm, config)).Methods("POST")
	router.PathPrefix("/{index}/_bulk").HandlerFunc(bulkVar(lm, config)).Methods("POST")
	router.PathPrefix("/{index}/_search").HandlerFunc(searchVar(lm, quesmaManagementConsole)).Methods("POST")
	return router
}

func writeSearchResponse(ctx context.Context, writer http.ResponseWriter, body []byte, err error) {
	_ = ctx
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(body)
}

func search(lm *clickhouse.LogManager, quesmaManagementConsole *QuesmaManagementConsole) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		responseBody, err := handleSearch(r.Context(), "", body, lm, quesmaManagementConsole)
		writeSearchResponse(r.Context(), writer, responseBody, err)
	})
}

func asyncSearch(lm *clickhouse.LogManager, quesmaManagementConsole *QuesmaManagementConsole) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		responseBody, err := handleAsyncSearch(r.Context(), "", body, lm, quesmaManagementConsole)
		writeSearchResponse(r.Context(), writer, responseBody, err)
	})
}

func searchVar(lm *clickhouse.LogManager, quesmaManagementConsole *QuesmaManagementConsole) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		responseBody, err := handleSearch(r.Context(), vars["index"], body, lm, quesmaManagementConsole)
		writeSearchResponse(r.Context(), writer, responseBody, err)
	})
}

func index(lm *clickhouse.LogManager, config config.QuesmaConfiguration) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		go dualWrite(r.Context(), vars["index"], string(body), lm, config)
	})
}

func bulk(lm *clickhouse.LogManager, config config.QuesmaConfiguration) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		go dualWriteBulk(r.Context(), "", string(body), lm, config)
	})
}

func bulkVar(lm *clickhouse.LogManager, config config.QuesmaConfiguration) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		go dualWriteBulk(r.Context(), mux.Vars(r)["index"], string(body), lm, config)
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
