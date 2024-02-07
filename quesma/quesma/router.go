package quesma

import (
	"bytes"
	"context"
	"github.com/gorilla/mux"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"net/http"
	"strings"
)

const (
	HealthPath          = "/_cluster/health"
	BulkPath            = "/_bulk"
	CreateTablePath     = "/_createTable"
	InsertPath          = "/_insert"
	SearchPath          = "/_search"
	AsyncSearchPath     = "/_async_search"
	ElasticInternalPath = "/_"
)

func bodyHandler(h func(body []byte, writer http.ResponseWriter, r *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tracing.RequestIdCtxKey, r.Header.Get("RequestId"))
		r = r.WithContext(ctx)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		r.Body = io.NopCloser(bytes.NewBuffer(body))
		h(body, writer, r)
	}
}

func configureRouting(config config.QuesmaConfiguration, lm *clickhouse.LogManager, quesmaManagementConsole *ui.QuesmaManagementConsole) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc(HealthPath, ok)
	router.PathPrefix(CreateTablePath).HandlerFunc(createTable(lm))
	router.PathPrefix(InsertPath).HandlerFunc(processInsert(lm))
	router.PathPrefix(BulkPath).HandlerFunc(bulk(lm, config)).Methods("POST")
	router.PathPrefix(SearchPath).HandlerFunc(search(lm, quesmaManagementConsole)).Methods("POST")
	router.PathPrefix("/{index}" + AsyncSearchPath).HandlerFunc(asyncSearch(lm, quesmaManagementConsole)).Methods("POST")
	router.PathPrefix(ElasticInternalPath).HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		logger.Debug().Msgf("unrecognized internal path: %s", r.RequestURI)
	})
	router.PathPrefix("/.").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		logger.Debug().Msgf("internal index access '%s', ignoring...", strings.Split(r.RequestURI, "/")[1])
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

func search(lm *clickhouse.LogManager, quesmaManagementConsole *ui.QuesmaManagementConsole) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		responseBody, err := handleSearch(r.Context(), "", body, lm, quesmaManagementConsole)
		writeSearchResponse(r.Context(), writer, responseBody, err)
	})
}

func asyncSearch(lm *clickhouse.LogManager, quesmaManagementConsole *ui.QuesmaManagementConsole) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		responseBody, err := handleAsyncSearch(r.Context(), "", body, lm, quesmaManagementConsole)
		writeSearchResponse(r.Context(), writer, responseBody, err)
	})
}

func searchVar(lm *clickhouse.LogManager, quesmaManagementConsole *ui.QuesmaManagementConsole) func(http.ResponseWriter, *http.Request) {
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
			logger.Error().Msg(err.Error())
		}
	})
}

func createTable(lm *clickhouse.LogManager) func(http.ResponseWriter, *http.Request) {
	return bodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		logger.Info().Msgf("%s --> create table\n", r.RequestURI)
		_ = lm.ProcessCreateTableQuery(string(body), clickhouse.NewDefaultCHConfig())
	})
}

func ok(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(200)
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(`{"cluster_name": "quesma"}`))
}
