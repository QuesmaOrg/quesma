package ui

import (
	"github.com/rs/zerolog"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/schema"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/util"

	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/stats"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	maxLastMessages         = 10000
	maxLogMessagePerRequest = 1000
)

const (
	RequestStatisticKibana2Clickhouse    = "kibana2clickhouse"
	RequestStatisticKibana2Elasticsearch = "kibana2elasticsearch"
	RequestStatisticIngest2Clickhouse    = "ingest2clickhouse"
	RequestStatisticIngest2Elasticsearch = "ingest2elasticsearch"
)

var requestIdRegex, _ = regexp.Compile(logger.RID + `":"([0-9a-fA-F-]+)"`)

type QueryDebugPrimarySource struct {
	Id          string
	QueryResp   []byte
	PrimaryTook time.Duration
}

type QueryDebugSecondarySource struct {
	Id string

	Path              string
	IncomingQueryBody []byte

	QueryBodyTranslated    []byte
	QueryTranslatedResults []byte
	SecondaryTook          time.Duration
}

type queryDebugInfo struct {
	QueryDebugPrimarySource
	QueryDebugSecondarySource
	logMessages   []string
	errorLogCount int
	warnLogCount  int
	unsupported   *string
}

type queryDebugInfoWithId struct {
	id    string
	query queryDebugInfo
}

type recordRequests struct {
	typeName string
	took     time.Duration
	error    bool
}

type (
	QuesmaManagementConsole struct {
		queryDebugPrimarySource   chan *QueryDebugPrimarySource
		queryDebugSecondarySource chan *QueryDebugSecondarySource
		queryDebugLogs            <-chan logger.LogWithLevel
		ui                        *http.Server
		mutex                     sync.Mutex
		debugInfoMessages         map[string]queryDebugInfo
		debugLastMessages         []string
		responseMatcherChannel    chan queryDebugInfo
		cfg                       config.QuesmaConfiguration
		requestsStore             *stats.RequestStatisticStore
		requestsSource            chan *recordRequests
		startedAt                 time.Time
		clickhouseStatusCache     healthCheckStatusCache
		elasticStatusCache        healthCheckStatusCache
		logManager                *clickhouse.LogManager
		indexManagement           elasticsearch.IndexManagement
		phoneHomeAgent            telemetry.PhoneHomeAgent
		schemasProvider           SchemasProvider
		totalUnsupportedQueries   int
	}
	SchemasProvider interface {
		AllSchemas() map[schema.TableName]schema.Schema
	}
)

func NewQuesmaManagementConsole(config config.QuesmaConfiguration, logManager *clickhouse.LogManager, indexManager elasticsearch.IndexManagement, logChan <-chan logger.LogWithLevel, phoneHomeAgent telemetry.PhoneHomeAgent, schemasProvider SchemasProvider) *QuesmaManagementConsole {
	return &QuesmaManagementConsole{
		queryDebugPrimarySource:   make(chan *QueryDebugPrimarySource, 10),
		queryDebugSecondarySource: make(chan *QueryDebugSecondarySource, 10),
		queryDebugLogs:            logChan,
		debugInfoMessages:         make(map[string]queryDebugInfo),
		debugLastMessages:         make([]string, 0),
		responseMatcherChannel:    make(chan queryDebugInfo, 5),
		cfg:                       config,
		requestsStore:             stats.NewRequestStatisticStore(),
		requestsSource:            make(chan *recordRequests, 100),
		startedAt:                 time.Now(),
		clickhouseStatusCache:     newHealthCheckStatusCache(),
		elasticStatusCache:        newHealthCheckStatusCache(),
		logManager:                logManager,
		indexManagement:           indexManager,
		phoneHomeAgent:            phoneHomeAgent,
		schemasProvider:           schemasProvider,
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

func (qdi *queryDebugInfo) requestContains(queryStr string) bool {
	potentialPlaces := [][]byte{qdi.QueryDebugSecondarySource.IncomingQueryBody,
		qdi.QueryDebugSecondarySource.QueryBodyTranslated}
	for _, potentialPlace := range potentialPlaces {
		if potentialPlace != nil && strings.Contains(string(potentialPlace), queryStr) {
			return true
		}
	}
	return false
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
		debugPrimaryInfo := QueryDebugPrimarySource{msg.Id,
			[]byte(util.JsonPrettify(string(msg.QueryResp), true)), msg.PrimaryTook}
		qmc.mutex.Lock()
		if value, ok := qmc.debugInfoMessages[msg.Id]; !ok {
			qmc.debugInfoMessages[msg.Id] = queryDebugInfo{
				QueryDebugPrimarySource: debugPrimaryInfo,
			}
			qmc.addNewMessageId(msg.Id)
		} else {
			value.QueryDebugPrimarySource = debugPrimaryInfo
			qmc.debugInfoMessages[msg.Id] = value
			// That's the point where queryDebugInfo is
			// complete and we can compare results
			if isComplete(value) {
				qmc.responseMatcherChannel <- value
			}
		}
		qmc.mutex.Unlock()
	case msg := <-qmc.queryDebugSecondarySource:
		logger.Debug().Msg("Received debug info from secondary source: " + msg.Id)
		// fmt.Println(msg.IncomingQueryBody)
		secondaryDebugInfo := QueryDebugSecondarySource{
			msg.Id,
			msg.Path,
			[]byte(util.JsonPrettify(string(msg.IncomingQueryBody), true)),
			msg.QueryBodyTranslated,
			[]byte(util.JsonPrettify(string(msg.QueryTranslatedResults), true)),
			msg.SecondaryTook,
		}
		qmc.mutex.Lock()
		if value, ok := qmc.debugInfoMessages[msg.Id]; !ok {
			qmc.debugInfoMessages[msg.Id] = queryDebugInfo{
				QueryDebugSecondarySource: secondaryDebugInfo,
			}
			qmc.addNewMessageId(msg.Id)
		} else {
			value.QueryDebugSecondarySource = secondaryDebugInfo
			// That's the point where queryDebugInfo is
			// complete and we can compare results
			qmc.debugInfoMessages[msg.Id] = value
			if isComplete(value) {
				qmc.responseMatcherChannel <- value
			}
		}
		qmc.mutex.Unlock()
	case log := <-qmc.queryDebugLogs:
		match := requestIdRegex.FindStringSubmatch(log.Msg)
		if len(match) < 2 {
			// there's no request_id in the log message
			return
		}
		requestId := match[1]

		qmc.mutex.Lock()
		var value queryDebugInfo
		var ok bool
		if value, ok = qmc.debugInfoMessages[requestId]; !ok {
			value = queryDebugInfo{
				logMessages: []string{log.Msg},
			}
			qmc.addNewMessageId(requestId)
		} else {
			if len(value.logMessages) < maxLogMessagePerRequest {
				value.logMessages = append(value.logMessages, log.Msg)
			} else {
				lastMsg := `{"level":"error","message":"Max log messages reached"}`
				if value.logMessages[len(value.logMessages)-1] != lastMsg {
					value.logMessages = append(value.logMessages, lastMsg)
				}
			}
		}
		if log.Level == zerolog.ErrorLevel {
			value.errorLogCount += 1
		} else if log.Level == zerolog.WarnLevel {
			value.warnLogCount += 1
		}
		if unsupported := processUnsupportedLogMessage(log); unsupported != nil {
			value.unsupported = unsupported
			qmc.totalUnsupportedQueries += 1
		}

		qmc.debugInfoMessages[requestId] = value
		qmc.mutex.Unlock()
	case record := <-qmc.requestsSource:
		qmc.requestsStore.RecordRequest(record.typeName, record.took, record.error)
	}
}

func isComplete(value queryDebugInfo) bool {
	return !reflect.DeepEqual(value.QueryDebugPrimarySource, QueryDebugPrimarySource{}) && !reflect.DeepEqual(value.QueryDebugSecondarySource, QueryDebugSecondarySource{})
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

// RunOnlyChannelProcessor is a copy of Run() method, but runs the management console in a mode where it only processes channel messages
// Used only in tests, to keep the important part of the logic, but make them run faster.
func (qmc *QuesmaManagementConsole) RunOnlyChannelProcessor() {
	for {
		qmc.processChannelMessage()
	}
}

func (qmc *QuesmaManagementConsole) comparePipelines() {
	for {
		queryDebugInfo, ok := <-qmc.responseMatcherChannel
		if ok {
			if string(queryDebugInfo.QueryResp) != string(queryDebugInfo.QueryTranslatedResults) {
				if len(queryDebugInfo.QueryResp) == 0 {
					queryDebugInfo.QueryResp = []byte("{}")
				}
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
