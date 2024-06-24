package ui

import (
	"embed"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"net/http"
	"net/http/pprof"
	"quesma/logger"
	"quesma/stats"
	"runtime"
)

const (
	uiTcpPort              = "9999"
	managementInternalPath = "/_quesma"
	healthPath             = managementInternalPath + "/health"
)

//go:embed asset/*
var uiFs embed.FS

func (qmc *QuesmaManagementConsole) createRouting() *mux.Router {
	router := mux.NewRouter()

	router.Use(panicRecovery)

	router.HandleFunc(healthPath, qmc.checkHealth)

	qmc.initPprof(router)

	router.HandleFunc("/", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDashboard()
		_, _ = writer.Write(buf)
	})

	// /dashboard is referenced in docs and should redirect to /
	router.HandleFunc("/dashboard", func(writer http.ResponseWriter, req *http.Request) {
		http.Redirect(writer, req, "/", http.StatusSeeOther)
	})

	router.HandleFunc("/live", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateLiveTail()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/tables/reload", func(writer http.ResponseWriter, req *http.Request) {
		qmc.logManager.ReloadTables()
		buf := qmc.generateTables()
		_, _ = writer.Write(buf)
	}).Methods("POST")

	router.HandleFunc("/tables", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateTables()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/schemas", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateSchemas()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/telemetry", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateTelemetry()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/data-sources", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDatasourcesPage()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/routing-statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateRouterStatisticsLiveTail()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/ingest-statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateIngestStatistics()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/statistics-json", func(writer http.ResponseWriter, req *http.Request) {
		jsonBody, err := json.Marshal(stats.GlobalStatistics)
		if err != nil {
			logger.Error().Msgf("Error marshalling statistics: %v", err)
			writer.WriteHeader(500)
			return
		}
		_, _ = writer.Write(jsonBody)
		writer.WriteHeader(200)
	})

	router.HandleFunc("/panel/routing-statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateRouterStatistics()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/panel/statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateStatistics()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/panel/queries", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateQueries()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/panel/dashboard", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDashboardPanel()
		buf = append(buf, qmc.generateDashboardTrafficPanel()...)
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/panel/data-sources", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDatasources()
		_, _ = writer.Write(buf)
	})

	router.PathPrefix("/request-id/{requestId}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForRequestId(vars["requestId"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/error/{reason}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateErrorForReason(vars["reason"])
		_, _ = writer.Write(buf)
	})
	router.Path("/unsupported-requests").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		buf := qmc.generateReportForUnsupportedRequests()
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/unsupported-requests/{reason}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForUnsupportedType(vars["reason"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/requests-by-str/{queryString}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForRequestsWithStr(vars["queryString"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/requests-with-error/").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		buf := qmc.generateReportForRequestsWithError()
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/requests-with-warning/").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		buf := qmc.generateReportForRequestsWithWarning()
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/request-id").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		// redirect to /
		http.Redirect(writer, r, "/", http.StatusSeeOther)
	})
	router.PathPrefix("/requests-by-str").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		// redirect to /
		http.Redirect(writer, r, "/", http.StatusSeeOther)
	})
	router.HandleFunc("/queries", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateQueries()
		_, _ = writer.Write(buf)
	})

	router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.FS(uiFs))))
	return router
}

func (qmc *QuesmaManagementConsole) initPprof(router *mux.Router) {
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

func (qmc *QuesmaManagementConsole) newHTTPServer() *http.Server {
	return &http.Server{
		Addr:    ":" + uiTcpPort,
		Handler: qmc.createRouting(),
	}
}

func panicRecovery(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				buf = buf[:n]

				w.WriteHeader(http.StatusInternalServerError)
				w.Header().Set("Content-Type", "text/plain")
				w.Write([]byte("Internal Server Error\n\n"))

				w.Write([]byte("Stack:\n"))
				w.Write(buf)
				logger.Error().Msgf("recovering from err %v\n %s", err, buf)
			}
		}()

		h.ServeHTTP(w, r)
	})
}

func (qmc *QuesmaManagementConsole) checkHealth(writer http.ResponseWriter, _ *http.Request) {
	health := qmc.checkElasticsearch()
	if health.Status != "red" {
		writer.WriteHeader(200)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"cluster_name": "quesma"}`))
	} else {
		writer.WriteHeader(503)
		_, _ = writer.Write([]byte(`Elastic search is unavailable: ` + health.Message))
	}
}

func (qmc *QuesmaManagementConsole) listenAndServe() {
	if err := qmc.ui.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Msgf("Error starting server: %v", err)
	}
}
