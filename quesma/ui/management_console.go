// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/stats"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/diag"
	"github.com/rs/zerolog"
	"io"
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

type queryDebugInfo struct {
	diag.QueryDebugPrimarySource
	diag.QueryDebugSecondarySource
	alternativePlanDebugSecondarySource *diag.QueryDebugSecondarySource
	logMessages                         []string
	errorLogCount                       int
	warnLogCount                        int
	unsupported                         *string
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
		queryDebugPrimarySource   chan *diag.QueryDebugPrimarySource
		queryDebugSecondarySource chan *diag.QueryDebugSecondarySource
		queryDebugLogs            <-chan logger.LogWithLevel
		ui                        *http.Server
		mutex                     sync.Mutex
		debugInfoMessages         map[string]queryDebugInfo
		debugLastMessages         []string
		responseMatcherChannel    chan queryDebugInfo
		cfg                       *config.QuesmaConfiguration
		requestsStore             *stats.RequestStatisticStore
		requestsSource            chan *recordRequests
		startedAt                 time.Time
		clickhouseStatusCache     healthCheckStatusCache
		elasticStatusCache        healthCheckStatusCache
		logManager                *clickhouse.LogManager
		phoneHomeAgent            diag.PhoneHomeRecentStatsProvider
		schemasProvider           SchemasProvider
		totalUnsupportedQueries   int
		elasticsearch             *backend_connectors.ElasticsearchBackendConnector
		tableResolver             table_resolver.TableResolver

		isAuthEnabled bool
	}
	SchemasProvider interface {
		AllSchemas() map[schema.IndexName]schema.Schema
	}
)

func NewQuesmaManagementConsole(cfg *config.QuesmaConfiguration, logManager *clickhouse.LogManager, logChan <-chan logger.LogWithLevel, phoneHomeAgent diag.PhoneHomeRecentStatsProvider, schemasProvider SchemasProvider, indexRegistry table_resolver.TableResolver) *QuesmaManagementConsole {
	return &QuesmaManagementConsole{
		queryDebugPrimarySource:   make(chan *diag.QueryDebugPrimarySource, 10),
		queryDebugSecondarySource: make(chan *diag.QueryDebugSecondarySource, 10),
		queryDebugLogs:            logChan,
		debugInfoMessages:         make(map[string]queryDebugInfo),
		debugLastMessages:         make([]string, 0),
		responseMatcherChannel:    make(chan queryDebugInfo, 5),
		cfg:                       cfg,
		requestsStore:             stats.NewRequestStatisticStore(),
		requestsSource:            make(chan *recordRequests, 100),
		startedAt:                 time.Now(),
		clickhouseStatusCache:     newHealthCheckStatusCache(),
		elasticStatusCache:        newHealthCheckStatusCache(),
		logManager:                logManager,
		elasticsearch:             backend_connectors.NewElasticsearchBackendConnector(cfg.Elasticsearch),
		phoneHomeAgent:            phoneHomeAgent,
		schemasProvider:           schemasProvider,
		tableResolver:             indexRegistry,
	}
}

func (qmc *QuesmaManagementConsole) PushPrimaryInfo(qdebugInfo *diag.QueryDebugPrimarySource) {
	qmc.queryDebugPrimarySource <- qdebugInfo
}

func (qmc *QuesmaManagementConsole) PushSecondaryInfo(qdebugInfo *diag.QueryDebugSecondarySource) {
	qmc.queryDebugSecondarySource <- qdebugInfo
}

func (qmc *QuesmaManagementConsole) RecordRequest(typeName string, took time.Duration, error bool) {
	qmc.requestsSource <- &recordRequests{typeName, took, error}
}

func (qdi *queryDebugInfo) requestContains(queryStr string) bool {

	var translatedQueries [][]byte
	for _, translatedQuery := range qdi.QueryDebugSecondarySource.QueryBodyTranslated {
		translatedQueries = append(translatedQueries, translatedQuery.Query)
	}

	potentialPlaces := [][]byte{qdi.QueryDebugSecondarySource.IncomingQueryBody,
		bytes.Join(translatedQueries, []byte{})}

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
		debugPrimaryInfo := diag.QueryDebugPrimarySource{Id: msg.Id,
			QueryResp: []byte(util.JsonPrettify(string(msg.QueryResp), true)), PrimaryTook: msg.PrimaryTook}
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
		secondaryDebugInfo := diag.QueryDebugSecondarySource{
			Id:                     msg.Id,
			AsyncId:                msg.AsyncId,
			OpaqueId:               msg.OpaqueId,
			Path:                   msg.Path,
			IncomingQueryBody:      []byte(util.JsonPrettify(string(msg.IncomingQueryBody), true)),
			QueryBodyTranslated:    msg.QueryBodyTranslated,
			QueryTranslatedResults: []byte(util.JsonPrettify(string(msg.QueryTranslatedResults), true)),
			SecondaryTook:          msg.SecondaryTook,
			IsAlternativePlan:      msg.IsAlternativePlan,
		}
		qmc.mutex.Lock()

		setDebugInfo := func(info *queryDebugInfo, secondaryDebugInfo diag.QueryDebugSecondarySource) {
			if secondaryDebugInfo.IsAlternativePlan {
				info.alternativePlanDebugSecondarySource = &secondaryDebugInfo
			} else {
				info.QueryDebugSecondarySource = secondaryDebugInfo
			}
		}

		if value, ok := qmc.debugInfoMessages[msg.Id]; !ok {

			debugInfo := queryDebugInfo{}
			setDebugInfo(&debugInfo, secondaryDebugInfo)

			qmc.debugInfoMessages[msg.Id] = debugInfo
			qmc.addNewMessageId(msg.Id)
		} else {

			setDebugInfo(&value, secondaryDebugInfo)

			// That's the point where queryDebugInfo is
			// complete and we can compare results
			qmc.debugInfoMessages[msg.Id] = value

			if !msg.IsAlternativePlan && isComplete(value) {
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
	return !reflect.DeepEqual(value.QueryDebugPrimarySource, diag.QueryDebugPrimarySource{}) && !reflect.DeepEqual(value.QueryDebugSecondarySource, diag.QueryDebugSecondarySource{})
}

func (qmc *QuesmaManagementConsole) GetElasticSearchIndices(ctx context.Context) (indices []string) {
	sources := qmc.elasticsearchResolveIndexPattern(ctx, "*")
	for _, index := range sources.Indices {
		indices = append(indices, index.Name)
	}
	return
}

func (qmc *QuesmaManagementConsole) elasticsearchResolveIndexPattern(ctx context.Context, indexPattern string) (sources elasticsearch.Sources) {
	resp, err := qmc.elasticsearch.RequestWithHeaders(ctx, "GET", elasticsearch.ResolveIndexPattenPath(indexPattern), nil, nil)
	if err != nil {
		logger.InfoWithCtx(ctx).Msgf("Failed call Elasticsearch: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.InfoWithCtx(ctx).Msgf("Failed to read response from Elasticsearch: %v", err)
		return
	}
	err = json.Unmarshal(body, &sources)
	if err != nil {
		logger.InfoWithCtx(ctx).Msgf("Failed to parse response from Elasticsearch: %v", err)
		return
	}
	return
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
					logger.Debug().Str(logger.RID, queryDebugInfo.QueryDebugPrimarySource.Id).
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
