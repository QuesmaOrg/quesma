// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package telemetry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/buildinfo"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	telemetry_headers "github.com/QuesmaOrg/quesma/quesma/telemetry/headers"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/diag"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v3/mem"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"

	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/recovery"
	"github.com/QuesmaOrg/quesma/quesma/stats/errorstats"

	"runtime"
	"strings"
	"time"
)

const (
	warmupInterval    = 30 * time.Second
	phoneHomeInterval = 3600 * time.Second

	clickhouseTimeout = 10 * time.Second
	elasticTimeout    = 10 * time.Second
	phoneHomeTimeout  = 10 * time.Second

	statusOk    = "ok"
	statusNotOk = "fail"

	reportTypeOnSchedule = "on-schedule"
	reportTypeOnShutdown = "on-shutdown"

	// for local debugging purposes
	phoneHomeLocalEnabled = false // used initially for testing
	tablesInUsageReport   = 10
)

type PhoneHomeAgent interface {
	Start()
	Stop(ctx context.Context)

	diag.PhoneHomeClient
	diag.PhoneHomeRecentStatsProvider
}

type agent struct {
	ctx    context.Context
	cancel context.CancelFunc

	clickHouseDb quesma_api.BackendConnector
	config       *config.QuesmaConfiguration
	clientId     string

	instanceId string
	statedAt   time.Time
	hostname   string

	clickHouseQueryTimes   diag.DurationMeasurement
	clickHouseInsertsTimes diag.DurationMeasurement
	elasticReadTimes       diag.DurationMeasurement
	elasticWriteTimes      diag.DurationMeasurement

	elasticBypassedReadTimes  diag.DurationMeasurement
	elasticBypassedWriteTimes diag.DurationMeasurement

	ingestCounters    diag.MultiCounter
	userAgentCounters diag.MultiCounter

	failedRequestCollector func() int64

	recent            diag.PhoneHomeStats
	telemetryEndpoint *config.Url

	httpClient *http.Client
}

func generateInstanceID() string {
	instanceId, err := uuid.NewUUID()
	if err != nil {
		logger.Error().Err(err).Msg("Error generating instance id")
		return "unknown"
	}
	return instanceId.String()

}

func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		logger.Error().Err(err).Msg("Error getting hostname")
		return "unknown"
	}
	return name
}

func NewPhoneHomeAgent(configuration *config.QuesmaConfiguration, clickHouseDb quesma_api.BackendConnector, clientId string) PhoneHomeAgent {

	// TODO
	// this is a question, maybe we should inherit context from the caller
	// maybe the main function should be the one to cancel the context

	ctx, cancel := context.WithCancel(context.Background())

	return &agent{
		ctx:                       ctx,
		cancel:                    cancel,
		hostname:                  hostname(),
		instanceId:                generateInstanceID(),
		clickHouseDb:              clickHouseDb,
		config:                    configuration,
		clientId:                  clientId,
		clickHouseQueryTimes:      newDurationMeasurement(ctx),
		clickHouseInsertsTimes:    newDurationMeasurement(ctx),
		elasticReadTimes:          newDurationMeasurement(ctx),
		elasticWriteTimes:         newDurationMeasurement(ctx),
		elasticBypassedReadTimes:  newDurationMeasurement(ctx),
		elasticBypassedWriteTimes: newDurationMeasurement(ctx),

		ingestCounters:    NewMultiCounter(ctx, nil),
		userAgentCounters: NewMultiCounter(ctx, processUserAgent),
		telemetryEndpoint: configuration.QuesmaInternalTelemetryUrl,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Minute,
		},
	}
}

func (a *agent) FailedRequestsCollector(f func() int64) {
	a.failedRequestCollector = f
}

func (a *agent) ClickHouseQueryDuration() diag.DurationMeasurement {
	return a.clickHouseQueryTimes
}

func (a *agent) ClickHouseInsertDuration() diag.DurationMeasurement {
	return a.clickHouseInsertsTimes
}

func (a *agent) ElasticReadRequestsDuration() diag.DurationMeasurement {
	return a.elasticReadTimes
}

func (a *agent) ElasticWriteRequestsDuration() diag.DurationMeasurement {
	return a.elasticWriteTimes
}

func (a *agent) ElasticBypassedReadRequestsDuration() diag.DurationMeasurement {
	return a.elasticBypassedReadTimes
}

func (a *agent) ElasticBypassedWriteRequestsDuration() diag.DurationMeasurement {
	return a.elasticBypassedWriteTimes
}

func (a *agent) IngestCounters() diag.MultiCounter {
	return a.ingestCounters
}

func (a *agent) UserAgentCounters() diag.MultiCounter {
	return a.userAgentCounters
}

func (a *agent) RecentStats() (recent diag.PhoneHomeStats, available bool) {
	return a.recent, a.recent.TakenAt != 0
}

func (a *agent) collectClickHouseUsage(ctx context.Context, stats *diag.ClickHouseStats) error {
	// it counts whole clickhouse database, including system tables
	totalSummaryQuery := `
select 
       sum(rows) as rows,
       sum(bytes) as bytes_size
from system.parts
where active

`
	ctx, cancel := context.WithTimeout(ctx, clickhouseTimeout)
	defer cancel()

	rows, err := a.clickHouseDb.Query(ctx, totalSummaryQuery)

	if err != nil {

		// code: 60 means system.parts table is not found
		// Hydrolix does not support system.parts table.
		if strings.Contains(err.Error(), "code: 60") {
			return err
		}

		logger.WarnWithCtxAndReason(ctx, "No clickhouse diag").Err(err).Msg("Error getting diag from clickhouse.")
		return err
	}

	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&stats.NumberOfRows, &stats.DiskSpace)
		if err != nil {
			logger.WarnWithCtxAndReason(ctx, "No clickhouse diag").Err(err).Msg("Error getting diag from clickhouse.")
			return err
		}
	}

	if rows.Err() != nil {
		logger.WarnWithCtxAndReason(ctx, "No clickhouse diag").Err(rows.Err()).Msg("Error getting diag from clickhouse.")
		return rows.Err()
	}
	return nil
}

func (a *agent) collectClickHouseTableSizes(ctx context.Context) (int64, map[string]int64, error) {
	totalSize, tablesWithSizes, err := a.getTableSizes(a.ctx)
	if err != nil {
		logger.WarnWithCtx(ctx).Msgf("Error getting table sizes from clickhouse: %v", err)
		return 0, nil, err
	}
	return totalSize, tablesWithSizes, nil
}

func (a *agent) getDbInfoHash() string {
	dbUrl, dbName, dbUser := "", "default", "<no-user>"
	if a.config.ClickHouse.User != "" {
		dbUser = a.config.ClickHouse.User
	}
	if a.config.ClickHouse.Database != "" {
		dbName = a.config.ClickHouse.Database
	}
	if a.config.ClickHouse.Url != nil {
		dbUrl = a.config.ClickHouse.Url.String()
	}
	// we hash it to avoid leaking sensitive info
	dbInfoHash := sha256.Sum256([]byte(fmt.Sprintf("%s@%s/%s", dbUser, dbUrl, dbName)))
	return fmt.Sprintf("%x", dbInfoHash[:8])
}

func (a *agent) getTableSizes(ctx context.Context) (int64, map[string]int64, error) {
	tableSizes := make(map[string]int64)
	dbName := "default"
	allTablesSize := int64(0)
	if a.config.ClickHouse.Database != "" {
		dbName = a.config.ClickHouse.Database
	}
	query := `SELECT table, sum(bytes_on_disk) AS total_size
FROM system.parts
WHERE active = 1 AND database = ?
GROUP BY table
ORDER BY total_size DESC;`

	rows, err := a.clickHouseDb.Query(ctx, query, dbName)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		var totalSize int64
		if err := rows.Scan(&tableName, &totalSize); err != nil {
			return 0, nil, fmt.Errorf("failed to scan row: %w", err)
		}
		tableSize := totalSize / 1000000 // convert bytes to megabytes
		if tableSize >= 1 {              // we're not interested in tables smaller than 1MB
			tableSizes[tableName] = tableSize
		}
		allTablesSize += tableSize
	}
	tableSizes = getTopNValues(tableSizes, tablesInUsageReport)

	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("error iterating over rows: %w", err)
	}
	return allTablesSize, tableSizes, nil
}

func getTopNValues(in map[string]int64, n int) map[string]int64 {
	type kv struct {
		Key   string
		Value int64
	}
	var sortedSlice []kv
	for k, v := range in {
		sortedSlice = append(sortedSlice, kv{k, v})
	}
	sort.Slice(sortedSlice, func(i, j int) bool {
		return sortedSlice[i].Value > sortedSlice[j].Value
	})
	result := make(map[string]int64) // get the top `n` values
	for i := 0; i < n && i < len(sortedSlice); i++ {
		result[sortedSlice[i].Key] = sortedSlice[i].Value
	}
	return result
}

func (a *agent) collectClickHouseVersion(ctx context.Context, stats *diag.ClickHouseStats) error {

	// https://clickhouse.com/docs/en/sql-reference/functions/other-functions#version
	totalSummaryQuery := `select version()`

	ctx, cancel := context.WithTimeout(ctx, clickhouseTimeout)
	defer cancel()

	rows, err := a.clickHouseDb.Query(ctx, totalSummaryQuery)

	if err != nil {
		logger.Error().Err(err).Msg("Error getting version from clickhouse.")
		return err
	}

	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&stats.ServerVersion)
		if err != nil {
			logger.Error().Err(err).Msg("Error getting version from clickhouse.")
			return err
		}
	}

	if rows.Err() != nil {
		logger.Error().Err(rows.Err()).Msg("Error getting version from clickhouse.")
		return rows.Err()
	}
	return nil
}

func (a *agent) CollectClickHouse(ctx context.Context) (stats diag.ClickHouseStats) {

	// https://gist.github.com/sanchezzzhak/511fd140e8809857f8f1d84ddb937015
	stats.Status = statusNotOk

	dbStats := a.clickHouseDb.Stats()

	stats.MaxOpenConnection = dbStats.MaxOpenConnections
	stats.OpenConnection = dbStats.OpenConnections

	if err := a.collectClickHouseUsage(ctx, &stats); err != nil {
		return stats
	}
	if err := a.collectClickHouseVersion(ctx, &stats); err != nil {
		return stats
	}

	stats.Status = statusOk

	return stats
}

type elasticStatsResponse struct {
	All struct {
		Total struct {
			Docs struct {
				Count int64 `json:"count"`
			} `json:"docs"`
			Store struct {
				SizeInBytes int64 `json:"size_in_bytes"`
			} `json:"store"`
		} `json:"total"`
	} `json:"_all"`
}

type elasticVersionResponse struct {
	Version struct {
		Number string `json:"number"`
	}
}

func (a *agent) callElastic(ctx context.Context, url *url.URL, response interface{}) (err error) {

	ctx, cancel := context.WithTimeout(ctx, elasticTimeout)
	defer cancel()

	request, err := a.buildElastisearchRequest(ctx, url, nil)

	if err != nil {
		logger.Error().Err(err).Msg("Error getting diag from elasticsearch. ")
		return err
	}

	resp, err := a.httpClient.Do(request)
	if err != nil {
		logger.Error().Err(err).Msg("Error getting info from elasticsearch. ")
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error().Msgf("Error getting info from elasticsearch. URL %s got status code: %v", url.String(), resp.StatusCode)
		return err
	}

	body, err := io.ReadAll(resp.Body)

	if err != nil {
		logger.Error().Err(err).Msg("Error getting info from elasticsearch. Reading the body failed")
		return err
	}

	err = json.Unmarshal(body, response)
	if err != nil {
		logger.Error().Err(err).Msg("Error getting info from elasticsearch. JSON parsing failed.")
		return err
	}

	return nil
}

func (a *agent) collectElasticUsage(ctx context.Context, stats *diag.ElasticStats) (err error) {
	// queries
	//curl  -s 'http://localhost:9200/_all/_stats?pretty=true' | jq ._all.total.docs
	//curl  -s 'http://localhost:9200/_all/_stats?pretty=true' | jq ._all.total.store

	elasticUrl := a.config.Elasticsearch.Url

	statsUrl := elasticUrl.JoinPath("/_all/_stats")
	response := elasticStatsResponse{}
	err = a.callElastic(ctx, statsUrl, &response)

	if err != nil {
		return err
	}

	stats.NumberOfDocs = response.All.Total.Docs.Count
	stats.Size = response.All.Total.Store.SizeInBytes

	return nil
}

func (a *agent) collectElasticVersion(ctx context.Context, stats *diag.ElasticStats) (err error) {

	elasticUrl := a.config.Elasticsearch.Url

	statsUrl := elasticUrl.JoinPath("/")
	response := &elasticVersionResponse{}
	err = a.callElastic(ctx, statsUrl, &response)

	if err != nil {
		return err
	}

	stats.ServerVersion = response.Version.Number

	return nil
}

func (a *agent) collectElasticHealthStatus(ctx context.Context, stats *diag.ElasticStats) (err error) {

	healthChecker := elasticsearch.NewElasticHealthChecker(a.config)

	stats.HealthStatus = healthChecker.CheckHealth().String()

	return nil
}

func (a *agent) CollectElastic(ctx context.Context) (stats diag.ElasticStats) {

	stats.Status = statusNotOk
	stats.HealthStatus = "n/a"

	err := a.collectElasticHealthStatus(ctx, &stats)
	if err != nil {
		return stats
	}

	err = a.collectElasticVersion(ctx, &stats)
	if err != nil {
		return stats
	}

	err = a.collectElasticUsage(ctx, &stats)
	if err != nil {
		return stats
	}

	stats.Status = statusOk
	return stats
}

func (a *agent) buildElastisearchRequest(ctx context.Context, statsUrl *url.URL, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statsUrl.String(), body)
	if err != nil {
		return nil, err
	}
	req = elasticsearch.AddBasicAuthIfNeeded(req, a.config.Elasticsearch.User, a.config.Elasticsearch.Password)
	return req, nil
}

func (a *agent) runtimeStats() (stats diag.RuntimeStats) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	stats.MemoryUsed = m.Alloc
	if v, errV := mem.VirtualMemory(); errV == nil {
		stats.MemoryAvailable = v.Total
	}

	stats.NumberOfCPUs = runtime.NumCPU()
	stats.NumberOfGoroutines = runtime.NumGoroutine()
	stats.NumberOfGC = m.NumGC

	return stats
}

func (a *agent) collect(ctx context.Context, reportType string) (stats diag.PhoneHomeStats) {
	// FIXME: this should log the pipelines used, not phased-out modes
	if a.config.TransparentProxy {
		stats.ConfigMode = "proxy-inspect"
	} else {
		stats.ConfigMode = "dual-write-query-clickhouse"
	}

	stats.ReportType = reportType
	stats.Hostname = a.hostname
	stats.AgentStartedAt = a.statedAt.Unix()
	stats.TakenAt = time.Now().Unix()
	stats.QuesmaVersion = buildinfo.Version
	stats.BuildHash = buildinfo.BuildHash
	stats.BuildDate = buildinfo.BuildDate
	stats.NumberOfPanics = recovery.PanicCounter.Load()
	stats.InstanceID = a.instanceId

	stats.ClickHouseQueriesDuration = a.ClickHouseQueryDuration().AggregateAndReset()
	stats.ClickHouseInsertsDuration = a.ClickHouseInsertDuration().AggregateAndReset()
	stats.ElasticReadsDuration = a.ElasticReadRequestsDuration().AggregateAndReset()
	stats.ElasticWritesDuration = a.ElasticWriteRequestsDuration().AggregateAndReset()
	stats.ElasticBypassedReadsDuration = a.ElasticBypassedReadRequestsDuration().AggregateAndReset()
	stats.ElasticBypassedWritesDuration = a.ElasticBypassedWriteRequestsDuration().AggregateAndReset()
	stats.UserAgentCounters = a.userAgentCounters.AggregateTopValuesAndReset()

	stats.Elasticsearch = a.CollectElastic(ctx)

	if stats.ClickHouseInsertsDuration.Count > 0 || stats.ClickHouseQueriesDuration.Count > 0 {
		stats.ClickHouse = a.CollectClickHouse(ctx)
	} else {
		stats.ClickHouse = diag.ClickHouseStats{Status: "paused"}
	}
	if !strings.HasPrefix(a.config.ClickHouse.ConnectorType, "hydrolix") { // we only check table sizes for ClickHouse
		if totalSize, topTableSizes, err := a.collectClickHouseTableSizes(ctx); err == nil {
			stats.ClickHouse.DbInfoHash = a.getDbInfoHash()
			stats.ClickHouse.BillableSize = totalSize
			stats.ClickHouse.TopTablesSizeInfo = fmt.Sprintf("%v", topTableSizes)
			logger.Info().Msgf("[USAGE REPORT] dababase=[%s] billable_size_in_Mbs=[%d] top_table_sizes=%v", a.getDbInfoHash(), totalSize, topTableSizes)
		}
	}

	stats.IngestCounters = a.ingestCounters.AggregateAndReset()

	stats.RuntimeStats = a.runtimeStats()
	stats.TopErrors = a.topErrors()

	if a.failedRequestCollector != nil {
		stats.NumberOfFailedRequests = a.failedRequestCollector()
	}

	return stats
}

func (a *agent) topErrors() []string {
	var errors []string
	for _, e := range errorstats.GlobalErrorStatistics.ReturnTopErrors(10) {
		errors = append(errors, e.Reason)
	}
	return errors
}

func (a *agent) phoneHomeRemoteEndpoint(ctx context.Context, body []byte) (err error) {

	ctx, cancel := context.WithTimeout(ctx, phoneHomeTimeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.telemetryEndpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "quesma/"+buildinfo.Version)
	request.Header.Set(telemetry_headers.ClientId, a.clientId)

	resp, err := a.httpClient.Do(request)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("phone home failed, invalid status code: %v", resp.StatusCode)
	}

	return nil
}

func (a *agent) phoneHomeLocalQuesma(ctx context.Context, body []byte) (err error) {

	ctx, cancel := context.WithTimeout(ctx, phoneHomeTimeout)
	defer cancel()

	phoneHomeUrl := "http://localhost:8080/_bulk"

	bulkJson := `{"create":{"_index":"phone_home_data"}}`
	var elasticPayload []byte

	elasticPayload = append(elasticPayload, []byte(bulkJson)...)
	elasticPayload = append(elasticPayload, []byte("\n")...)
	elasticPayload = append(elasticPayload, body...)
	elasticPayload = append(elasticPayload, []byte("\n")...)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, phoneHomeUrl, bytes.NewReader(elasticPayload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "quesma/"+buildinfo.Version)
	request.Header.Set(telemetry_headers.ClientId, a.clientId)

	resp, err := a.httpClient.Do(request)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("phone home failed, invalid status code: %v", resp.StatusCode)
	}

	return nil
}

func (a *agent) report(ctx context.Context, stats diag.PhoneHomeStats) {

	data, err := json.Marshal(stats)
	if err != nil {
		logger.Error().Err(err).Msgf("Error marshalling diag")
		return
	}

	if a.telemetryEndpoint != nil {
		err = a.phoneHomeRemoteEndpoint(ctx, data)
		if err != nil {
			logger.Error().Msgf("Phone Home failed with error %v", err)
			logger.Info().Msgf("Phone Home: %v", string(data))
		} else {
			logger.Info().Msgf("Phone Home succeded.")
		}
	} else {
		logger.Warn().Msg("Remote telemetry endpoint is not set.")
	}

	if phoneHomeLocalEnabled {
		err = a.phoneHomeLocalQuesma(ctx, data)
		if err != nil {
			logger.Error().Msgf("Phone Home to itself failed with error %v", err)
			logger.Info().Msgf("Phone Home: %v", string(data))
		} else {
			logger.Info().Msgf("Phone Home to itself succeded.")
		}
	}

}

func (a *agent) telemetryCollection(ctx context.Context, reportType string) {

	// if we fail we would not die
	defer recovery.LogPanic()

	stats := a.collect(ctx, reportType)

	a.report(ctx, stats)

	a.recent = stats

}

func (a *agent) loop() {

	// do not collect diag immediately
	// wait for a while to let the system settle
	select {
	case <-a.ctx.Done():
		logger.Debug().Msg("agent stopped on warm up")
		return
	case <-time.After(warmupInterval):
	}

	for {
		logger.Debug().Msg("agent cycle")

		a.telemetryCollection(a.ctx, reportTypeOnSchedule)

		select {
		case <-a.ctx.Done():
			logger.Debug().Msg("agent interrupted")
			return
		case <-time.After(phoneHomeInterval):
		}
	}

}

func (a *agent) Start() {

	a.statedAt = time.Now()
	go a.loop()
	logger.Info().Msg("PhoneHomeAgent Started")

}

func (a *agent) Stop(ctx context.Context) {
	// stop the loop and all goroutines
	a.cancel()

	// collect the last diag using given context
	a.telemetryCollection(ctx, reportTypeOnShutdown)

	// stop all

	logger.Info().Msg("PhoneHomeAgent Stopped")

}
