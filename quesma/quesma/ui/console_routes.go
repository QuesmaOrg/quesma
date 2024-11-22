// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package ui

import (
	"embed"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"github.com/gorilla/securecookie"
	"github.com/gorilla/sessions"
	"github.com/markbates/goth"
	"github.com/markbates/goth/gothic"
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
	loginWithElasticSearch = "/login-with-elasticsearch"
)

//go:embed asset/*
var uiFs embed.FS

const quesmaSessionName = "quesma-session"

func init() {

	// Here we generate a random key for the session store
	// TODO We should use a secure key from the environment on production.
	// 32 - is a default key length, taken for example
	gothic.Store = sessions.NewCookieStore(securecookie.GenerateRandomKey(32))
}

func authCallbackHandler(w http.ResponseWriter, r *http.Request) {
	user, err := gothic.CompleteUserAuth(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	session, _ := store.Get(r, quesmaSessionName)
	session.Values["userID"] = user.UserID
	if err := session.Save(r, w); err != nil {
		logger.Error().Msgf("Error saving session: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
}

func (qmc *QuesmaManagementConsole) createRouting() *mux.Router {
	router := mux.NewRouter()

	router.Use(panicRecovery)

	router.HandleFunc(healthPath, qmc.checkHealth)

	qmc.initPprof(router)

	// just for oauth compliance
	router.HandleFunc("/auth/{provider}", gothic.BeginAuthHandler)
	router.HandleFunc("/auth/{provider}/callback", authCallbackHandler)

	// our logic for login
	router.HandleFunc(loginWithElasticSearch, qmc.HandleElasticsearchLogin)

	authenticatedRoutes := router.PathPrefix("/").Subrouter()
	if qmc.cfg.Elasticsearch.User == "" && qmc.cfg.Elasticsearch.Password == "" {
		logger.Warn().Msg("admin console authentication is disabled")
	} else {
		qmc.isAuthEnabled = true
		authenticatedRoutes.Use(authMiddleware)
	}

	authenticatedRoutes.HandleFunc("/", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDashboard()
		_, _ = writer.Write(buf)
	})

	// /dashboard is referenced in docs and should redirect to /
	authenticatedRoutes.HandleFunc("/dashboard", func(writer http.ResponseWriter, req *http.Request) {
		http.Redirect(writer, req, "/", http.StatusSeeOther)
	})

	authenticatedRoutes.HandleFunc("/live", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateLiveTail()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/table_resolver", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateTableResolver()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/table_resolver/ask", func(writer http.ResponseWriter, req *http.Request) {
		prompt := req.PostFormValue("prompt")

		buf := qmc.generateTableResolverAnswer(prompt)
		_, _ = writer.Write(buf)
	})

	checkIfAbAvailable := func(writer http.ResponseWriter, req *http.Request) bool {
		if qmc.hasABTestingTable() {
			return true
		}

		_, _ = writer.Write([]byte("AB Testing results are not available."))
		return false
	}

	authenticatedRoutes.HandleFunc(abTestingPath, func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateABTestingDashboard()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc(abTestingPath+"/report", func(writer http.ResponseWriter, req *http.Request) {
		if checkIfAbAvailable(writer, req) {
			kibanaUrl := req.PostFormValue("kibana_url")
			orderBy := req.PostFormValue("order_by")
			buf := qmc.generateABTestingReport(kibanaUrl, orderBy)
			_, _ = writer.Write(buf)
		}
	})

	authenticatedRoutes.HandleFunc(abTestingPath+"/panel", func(writer http.ResponseWriter, req *http.Request) {
		if checkIfAbAvailable(writer, req) {
			dashboardId := req.FormValue("dashboard_id")
			panelId := req.FormValue("panel_id")

			buf := qmc.generateABPanelDetails(dashboardId, panelId)
			_, _ = writer.Write(buf)
		}
	})

	authenticatedRoutes.HandleFunc(abTestingPath+"/mismatch", func(writer http.ResponseWriter, req *http.Request) {
		if checkIfAbAvailable(writer, req) {
			dashboardId := req.FormValue("dashboard_id")
			panelId := req.FormValue("panel_id")
			mismatchId := req.FormValue("mismatch_id")

			buf := qmc.generateABMismatchDetails(dashboardId, panelId, mismatchId)
			_, _ = writer.Write(buf)
		}
	})

	authenticatedRoutes.HandleFunc(abTestingPath+"/request", func(writer http.ResponseWriter, req *http.Request) {
		if checkIfAbAvailable(writer, req) {
			requestId := req.FormValue("request_id")
			buf := qmc.generateABSingleRequest(requestId)
			_, _ = writer.Write(buf)
		}
	})

	authenticatedRoutes.HandleFunc("/tables/reload", func(writer http.ResponseWriter, req *http.Request) {

		qmc.logManager.ReloadTables()
		buf := qmc.generateTables()
		_, _ = writer.Write(buf)
	}).Methods("POST")

	authenticatedRoutes.HandleFunc("/tables", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateTables()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/tables/common_table_stats", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateQuesmaAllLogs()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/schemas", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateSchemas()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/telemetry", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateTelemetry()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/data-sources", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDatasourcesPage()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/routing-statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateRouterStatisticsLiveTail()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/ingest-statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateIngestStatistics()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/statistics-json", func(writer http.ResponseWriter, req *http.Request) {
		jsonBody, err := json.Marshal(stats.GlobalStatistics)
		if err != nil {
			logger.Error().Msgf("Error marshalling statistics: %v", err)
			writer.WriteHeader(500)
			return
		}
		_, _ = writer.Write(jsonBody)
		writer.WriteHeader(200)
	})

	authenticatedRoutes.HandleFunc("/panel/routing-statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateRouterStatistics()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/panel/statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateStatistics()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/panel/queries", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateQueries()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/panel/dashboard", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDashboardPanel()
		buf = append(buf, qmc.generateDashboardTrafficPanel()...)
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.HandleFunc("/panel/data-sources", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDatasources()
		_, _ = writer.Write(buf)
	})

	authenticatedRoutes.PathPrefix("/request-id/{requestId}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForRequestId(vars["requestId"])
		_, _ = writer.Write(buf)
	})
	authenticatedRoutes.PathPrefix("/error/{reason}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateErrorForReason(vars["reason"])
		_, _ = writer.Write(buf)
	})
	authenticatedRoutes.Path("/unsupported-requests").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		buf := qmc.generateReportForUnsupportedRequests()
		_, _ = writer.Write(buf)
	})
	authenticatedRoutes.PathPrefix("/unsupported-requests/{reason}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForUnsupportedType(vars["reason"])
		_, _ = writer.Write(buf)
	})
	authenticatedRoutes.PathPrefix("/requests-by-str/{queryString}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForRequestsWithStr(vars["queryString"])
		_, _ = writer.Write(buf)
	})
	authenticatedRoutes.PathPrefix("/requests-with-error/").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		buf := qmc.generateReportForRequestsWithError()
		_, _ = writer.Write(buf)
	})
	authenticatedRoutes.PathPrefix("/requests-with-warning/").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		buf := qmc.generateReportForRequestsWithWarning()
		_, _ = writer.Write(buf)
	})
	authenticatedRoutes.PathPrefix("/request-id").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		// redirect to /
		http.Redirect(writer, r, "/", http.StatusSeeOther)
	})
	authenticatedRoutes.PathPrefix("/requests-by-str").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		// redirect to /
		http.Redirect(writer, r, "/", http.StatusSeeOther)
	})
	authenticatedRoutes.HandleFunc("/queries", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateQueries()
		_, _ = writer.Write(buf)
	})
	authenticatedRoutes.HandleFunc("/logout", func(writer http.ResponseWriter, req *http.Request) {
		session, err := store.Get(req, quesmaSessionName)
		if err != nil {
			http.Redirect(writer, req, "/login", http.StatusTemporaryRedirect)
			return
		}
		session.Options.MaxAge = -1
		session.Values = make(map[interface{}]interface{})
		err = session.Save(req, writer)
		if err != nil {
			logger.Error().Msgf("Could not delete user session: %v", err)
		}
		http.Redirect(writer, req, "/dashboard", http.StatusTemporaryRedirect)
	})

	authenticatedRoutes.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.FS(uiFs))))
	return router
}

func (qmc *QuesmaManagementConsole) initPprof(router *mux.Router) {
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
	router.HandleFunc("/debug/pprof/block", pprof.Handler("block").ServeHTTP)
	router.HandleFunc("/debug/pprof/mutex", pprof.Handler("mutex").ServeHTTP)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// Here we generate keys for the session store.
// TODO We should use a secure key from the environment on production.
// 32,64 are default key lengths.
var authKey = securecookie.GenerateRandomKey(64)
var encryptionKey = securecookie.GenerateRandomKey(32)
var store = sessions.NewCookieStore(authKey, encryptionKey)

func init() { // Safari does not allow Secure cookies on localhost
	store.Options = &sessions.Options{
		Secure: false,
	}
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isAlreadyAuthenticated(r) {
			logger.Warn().Msgf("User not authenticated, redirecting to login page")
			http.Redirect(w, r, "/auth/elasticsearch", http.StatusTemporaryRedirect)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isAlreadyAuthenticated(r *http.Request) bool {
	session, err := store.Get(r, quesmaSessionName)
	userID, ok := session.Values["userID"].(string)
	return ok && userID != "" && err == nil
}

func (qmc *QuesmaManagementConsole) newHTTPServer() *http.Server {
	goth.UseProviders(
		NewElasticsearchAuthProvider(),
	)
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
