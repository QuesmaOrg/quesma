package ui

import (
	"embed"
	"encoding/json"
	"errors"
	"github.com/mjibson/sqlfmt"
	"io"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/util"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/gorilla/mux"
)

const (
	uiTcpPort       = "9999"
	maxLastMessages = 10000
)

const (
	managementInternalPath = "/_quesma"
	healthPath             = managementInternalPath + "/health"
	bypassPath             = managementInternalPath + "/bypass"
)

const (
	RequestStatisticKibana2Clickhouse    = "kibana2clickhouse"
	RequestStatisticKibana2Elasticsearch = "kibana2elasticsearch"
	RequestStatisticIngest2Clickhouse    = "ingest2clickhouse"
	RequestStatisticIngest2Elasticsearch = "ingest2elasticsearch"
)

var requestIdRegex, _ = regexp.Compile(`request_id":"(\d+)"`)

//go:embed asset/*
var uiFs embed.FS

type QueryDebugPrimarySource struct {
	Id        string
	QueryResp []byte
}

type QueryDebugSecondarySource struct {
	Id string

	IncomingQueryBody []byte

	QueryBodyTranslated    []byte
	QueryRawResults        []byte
	QueryTranslatedResults []byte
}

type QueryDebugInfo struct {
	QueryDebugPrimarySource
	QueryDebugSecondarySource
	log string
}

type recordRequests struct {
	typeName string
	took     time.Duration
	error    bool
}

type QuesmaManagementConsole struct {
	queryDebugPrimarySource   chan *QueryDebugPrimarySource
	queryDebugSecondarySource chan *QueryDebugSecondarySource
	queryDebugLogs            <-chan string
	ui                        *http.Server
	mutex                     sync.Mutex
	debugInfoMessages         map[string]QueryDebugInfo
	debugLastMessages         []string
	responseMatcherChannel    chan QueryDebugInfo
	config                    config.QuesmaConfiguration
	requestsStore             *stats.RequestStatisticStore
	requestsSource            chan *recordRequests
	startedAt                 time.Time
	clickhouseStatusCache     healthCheckStatusCache
	elasticStatusCache        healthCheckStatusCache
	logManager                *clickhouse.LogManager
}

func NewQuesmaManagementConsole(config config.QuesmaConfiguration, logManager *clickhouse.LogManager, logChan <-chan string) *QuesmaManagementConsole {
	return &QuesmaManagementConsole{
		queryDebugPrimarySource:   make(chan *QueryDebugPrimarySource, 5),
		queryDebugSecondarySource: make(chan *QueryDebugSecondarySource, 5),
		queryDebugLogs:            logChan,
		debugInfoMessages:         make(map[string]QueryDebugInfo),
		debugLastMessages:         make([]string, 0),
		responseMatcherChannel:    make(chan QueryDebugInfo, 5),
		config:                    config,
		requestsStore:             stats.NewRequestStatisticStore(),
		requestsSource:            make(chan *recordRequests, 100),
		startedAt:                 time.Now(),
		clickhouseStatusCache:     newHealthCheckStatusCache(),
		elasticStatusCache:        newHealthCheckStatusCache(),
		logManager:                logManager,
	}
}

func (qmc *QuesmaManagementConsole) PushPrimaryInfo(qdebugInfo *QueryDebugPrimarySource) {
	qmc.queryDebugPrimarySource <- qdebugInfo
}

func (qmc *QuesmaManagementConsole) PushSecondaryInfo(qdebugInfo *QueryDebugSecondarySource) {
	qmc.queryDebugSecondarySource <- qdebugInfo
}

func (qmc *QuesmaManagementConsole) RecordRequest(typeName string, took time.Duration, error bool) {
	qmc.requestsSource <- &recordRequests{typeName, took, error}
}

func copyMap(originalMap map[string]QueryDebugInfo) map[string]QueryDebugInfo {
	copiedMap := make(map[string]QueryDebugInfo)

	for key, value := range originalMap {
		copiedMap[key] = value
	}

	return copiedMap
}

func (qdi *QueryDebugInfo) requestContains(queryStr string) bool {
	potentialPlaces := [][]byte{qdi.QueryDebugSecondarySource.IncomingQueryBody,
		qdi.QueryDebugSecondarySource.QueryBodyTranslated}
	for _, potentialPlace := range potentialPlaces {
		if potentialPlace != nil && strings.Contains(string(potentialPlace), queryStr) {
			return true
		}
	}
	return false
}

func (qmc *QuesmaManagementConsole) newHTTPServer() *http.Server {
	return &http.Server{
		Addr:    ":" + uiTcpPort,
		Handler: qmc.createRouting(),
	}
}

func (qmc *QuesmaManagementConsole) createRouting() *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc(healthPath, ok)

	router.HandleFunc(bypassPath, bypassSwitch).Methods("POST")

	router.HandleFunc("/", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateLiveTail()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/schema", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateSchema()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/routing-statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateRouterStatisticsLiveTail()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/ingest-statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateStatisticsLiveTail()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/dashboard", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDashboard()
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
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/panel/dashboard-traffic", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateDashboardTrafficPanel()
		_, _ = writer.Write(buf)
	})

	router.PathPrefix("/request-Id/{requestId}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForRequestId(vars["requestId"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/log/{requestId}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateLogForRequestId(vars["requestId"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/error/{reason}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateErrorForReason(vars["reason"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/requests-by-str/{queryString}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForRequests(vars["queryString"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/request-Id").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
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

func (qmc *QuesmaManagementConsole) listenAndServe() {
	if err := qmc.ui.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Msgf("Error starting server: %v", err)
	}
}

type DebugKeyValue struct {
	Key   string
	Value QueryDebugInfo
}

func sqlPrettyPrint(sqlData []byte) string {
	formattingConfig := tree.PrettyCfg{
		LineWidth:                120,
		DoNotNewLineAfterColName: true,
		Simplify:                 true,
		TabWidth:                 2,
		UseTabs:                  false,
		Align:                    tree.PrettyNoAlign,
	}
	stmts := strings.Split(strings.ReplaceAll(string(sqlData), "`", `"`), "\n") // sqlfmt can't deal with backticks
	sqlFormatted, err := sqlfmt.FmtSQL(formattingConfig, stmts)
	if err != nil {
		logger.Error().Msgf("Error while formatting sql: %v, stmts: %v", err, stmts)
		sqlFormatted = string(sqlData)
	}
	return sqlFormatted
}

func (qmc *QuesmaManagementConsole) generateQueries() []byte {
	// Take last MAX_LAST_MESSAGES to display, e.g. 100 out of potentially 10m000
	qmc.mutex.Lock()
	lastMessages := qmc.debugLastMessages
	debugKeyValueSlice := []DebugKeyValue{}
	count := 0
	for i := len(lastMessages) - 1; i >= 0 && count < maxLastMessages; i-- {
		debugInfoMessage := qmc.debugInfoMessages[lastMessages[i]]
		if len(debugInfoMessage.QueryDebugSecondarySource.IncomingQueryBody) > 0 {
			debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{lastMessages[i], debugInfoMessage})
			count++
		}
	}
	qmc.mutex.Unlock()

	return generateQueries(debugKeyValueSlice, true)
}

func newBufferWithHead() HtmlBuffer {
	const bufferSize = 4 * 1024 // size of ui/head.html
	var buffer HtmlBuffer
	buffer.Grow(bufferSize)
	head, err := uiFs.ReadFile("asset/head.html")
	buffer.Write(head)
	if err != nil {
		buffer.Text(err.Error())
	}
	buffer.Html("\n")
	return buffer
}

func (qmc *QuesmaManagementConsole) addNewMessageId(messageId string) {
	qmc.debugLastMessages = append(qmc.debugLastMessages, messageId)
	if len(qmc.debugLastMessages) > maxLastMessages {
		delete(qmc.debugInfoMessages, qmc.debugLastMessages[0])
		qmc.debugLastMessages = qmc.debugLastMessages[1:]
	}
}

func (qmc *QuesmaManagementConsole) processChannelMessage() {
	select {
	case msg := <-qmc.queryDebugPrimarySource:
		logger.Debug().Msg("Received debug info from primary source: " + msg.Id)
		debugPrimaryInfo := QueryDebugPrimarySource{msg.Id, msg.QueryResp}
		qmc.mutex.Lock()
		if value, ok := qmc.debugInfoMessages[msg.Id]; !ok {
			qmc.debugInfoMessages[msg.Id] = QueryDebugInfo{
				QueryDebugPrimarySource: debugPrimaryInfo,
			}
			qmc.addNewMessageId(msg.Id)
		} else {
			value.QueryDebugPrimarySource = debugPrimaryInfo
			qmc.debugInfoMessages[msg.Id] = value
			// That's the point where QueryDebugInfo is
			// complete and we can compare results
			qmc.responseMatcherChannel <- value
		}
		qmc.mutex.Unlock()
	case msg := <-qmc.queryDebugSecondarySource:
		logger.Debug().Msg("Received debug info from secondary source: " + msg.Id)
		secondaryDebugInfo := QueryDebugSecondarySource{
			msg.Id,
			msg.IncomingQueryBody,
			msg.QueryBodyTranslated,
			msg.QueryRawResults,
			msg.QueryTranslatedResults,
		}
		qmc.mutex.Lock()
		if value, ok := qmc.debugInfoMessages[msg.Id]; !ok {
			qmc.debugInfoMessages[msg.Id] = QueryDebugInfo{
				QueryDebugSecondarySource: secondaryDebugInfo,
			}
			qmc.addNewMessageId(msg.Id)
		} else {
			value.QueryDebugSecondarySource = secondaryDebugInfo
			// That's the point where QueryDebugInfo is
			// complete and we can compare results
			qmc.debugInfoMessages[msg.Id] = value
			qmc.responseMatcherChannel <- value
		}
		qmc.mutex.Unlock()
	case msg := <-qmc.queryDebugLogs:
		match := requestIdRegex.FindStringSubmatch(msg)
		if len(match) < 2 {
			// there's no request_id in the log message
			return
		}
		requestId := match[1]
		msgPretty := util.JsonPrettify(msg, false) + "\n"

		qmc.mutex.Lock()
		if value, ok := qmc.debugInfoMessages[requestId]; !ok {
			qmc.debugInfoMessages[requestId] = QueryDebugInfo{
				log: msgPretty,
			}
			qmc.addNewMessageId(requestId)
		} else {
			value.log += msgPretty
			qmc.debugInfoMessages[requestId] = value
		}
		qmc.mutex.Unlock()
	case record := <-qmc.requestsSource:
		qmc.requestsStore.RecordRequest(record.typeName, record.took, record.error)
	}
}

func (qmc *QuesmaManagementConsole) Run() {
	go qmc.comparePipelines()
	go func() {
		qmc.ui = qmc.newHTTPServer()
		qmc.listenAndServe()
	}()
	for {
		qmc.processChannelMessage()
	}
}

func ok(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(200)
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(`{"cluster_name": "quesma"}`))
}

// curl -X POST localhost:9999/_quesma/bypass -d '{"bypass": true}'
func bypassSwitch(writer http.ResponseWriter, r *http.Request) {
	bodyString, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error().Msgf("Error reading body: %v", err)
		writer.WriteHeader(400)
		_, _ = writer.Write([]byte("Error reading body: " + err.Error()))
		return
	}
	body := make(map[string]interface{})
	err = json.Unmarshal(bodyString, &body)
	if err != nil {
		logger.Fatal().Msg(err.Error())
	}

	if body["bypass"] != nil {
		val := body["bypass"].(bool)
		config.SetTrafficAnalysis(val)
		logger.Info().Msgf("global bypass set to %t\n", val)
		writer.WriteHeader(200)
	} else {
		writer.WriteHeader(400)
	}
}

func (qmc *QuesmaManagementConsole) comparePipelines() {
	for {
		queryDebugInfo, ok := <-qmc.responseMatcherChannel
		if ok {
			if string(queryDebugInfo.QueryResp) != string(queryDebugInfo.QueryTranslatedResults) {
				elasticSurplusFields, ourSurplusFields, err := util.JsonDifference(
					string(queryDebugInfo.QueryResp),
					string(queryDebugInfo.QueryTranslatedResults),
				)
				if err != nil {
					logger.Error().Str(logger.RID, queryDebugInfo.QueryDebugPrimarySource.Id).
						Msgf("Error while comparing responses: %v", err)
					continue
				}
				if len(elasticSurplusFields) > 0 || len(ourSurplusFields) > 0 {
					logger.Info().Str(logger.RID, queryDebugInfo.QueryDebugPrimarySource.Id).
						Msgf("Response structure different, extra keys:\n"+
							" Clickhouse response - Elastic response: %v\n"+
							" Elastic response - Clickhouse response: %v",
							ourSurplusFields, elasticSurplusFields)
				} else {
					logger.Debug().Str(logger.RID, queryDebugInfo.QueryDebugPrimarySource.Id).
						Msg("Responses are different, but src structure is the same")
				}
			}
		}
	}
}
