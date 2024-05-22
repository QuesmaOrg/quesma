package telemetry

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v3/mem"
	"io"
	"mitmproxy/quesma/buildinfo"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/stats/errorstats"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"time"
)

const (
	warmupInterval    = 30 * time.Second
	phoneHomeInterval = 3600 * time.Second

	clickhouseTimeout = 10 * time.Second
	elasticTimeout    = 10 * time.Second
	phoneHomeTimeout  = 10 * time.Second

	statusOk    = "ok"
	statusNotOk = "not-ok"

	reportTypeOnSchedule = "on-schedule"
	reportTypeOnShutdown = "on-shutdown"

	// for local debugging purposes
	phoneHomeLocalEnabled = false // used initially for testing
)

type ClickHouseStats struct {
	Status            string `json:"status"`
	NumberOfRows      int64  `json:"number_of_rows" db:"number_of_rows"`
	DiskSpace         int64  `json:"disk_space"`
	OpenConnection    int    `json:"open_connection"`
	MaxOpenConnection int    `json:"max_open_connection"`
	ServerVersion     string `json:"server_version"`
}

type ElasticStats struct {
	Status        string `json:"status"`
	NumberOfDocs  int64  `json:"number_of_docs"`
	Size          int64  `json:"size"`
	ServerVersion string `json:"server_version"`
}

type RuntimeStats struct {
	MemoryUsed      uint64 `json:"memory_used"`
	MemoryAvailable uint64 `json:"memory_available"`
}

type PhoneHomeStats struct {
	AgentStartedAt int64  `json:"started_at"`
	Hostname       string `json:"hostname"`
	QuesmaVersion  string `json:"quesma_version"`
	InstanceID     string `json:"instanceId"`

	// add more stats here about running

	ClickHouse    ClickHouseStats `json:"clickhouse"`
	Elasticsearch ElasticStats    `json:"elasticsearch"`

	ClickHouseQueriesDuration DurationStats `json:"clickhouse_queries"`
	ClickHouseInsertsDuration DurationStats `json:"clickhouse_inserts"`
	ElasticReadsDuration      DurationStats `json:"elastic_read_requests"`
	ElasticWritesDuration     DurationStats `json:"elastic_write_requests"`

	ElasticBypassedReadsDuration  DurationStats `json:"elastic_bypassed_read_requests"`
	ElasticBypassedWritesDuration DurationStats `json:"elastic_bypassed_write_requests"`

	// Due to schema issues, we are not using this for now, ref: https://github.com/QuesmaOrg/poc-elk-mitmproxy/pull/648
	IngestCounters    MultiCounterStats          `json:"-"`
	UserAgentCounters MultiCounterTopValuesStats `json:"top_user_agents"`

	RuntimeStats   RuntimeStats `json:"runtime"`
	NumberOfPanics int64        `json:"number_of_panics"`
	ReportType     string       `json:"report_type"`
	TakenAt        int64        `json:"taken_at"`
	ConfigMode     string       `json:"config_mode"`
	TopErrors      []string     `json:"top_errors"`
}

type PhoneHomeAgent interface {
	Start()
	Stop(ctx context.Context)

	RecentStats() (recent PhoneHomeStats, available bool)

	ClickHouseQueryDuration() DurationMeasurement
	ClickHouseInsertDuration() DurationMeasurement
	ElasticReadRequestsDuration() DurationMeasurement
	ElasticWriteRequestsDuration() DurationMeasurement

	ElasticBypassedReadRequestsDuration() DurationMeasurement
	ElasticBypassedWriteRequestsDuration() DurationMeasurement

	IngestCounters() MultiCounter
	UserAgentCounters() MultiCounter
}

type agent struct {
	ctx    context.Context
	cancel context.CancelFunc

	clickHouseDb *sql.DB
	config       config.QuesmaConfiguration

	instanceId string
	statedAt   time.Time
	hostname   string

	clickHouseQueryTimes   DurationMeasurement
	clickHouseInsertsTimes DurationMeasurement
	elasticReadTimes       DurationMeasurement
	elasticWriteTimes      DurationMeasurement

	elasticBypassedReadTimes  DurationMeasurement
	elasticBypassedWriteTimes DurationMeasurement

	ingestCounters    MultiCounter
	userAgentCounters MultiCounter

	recent            PhoneHomeStats
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

func NewPhoneHomeAgent(configuration config.QuesmaConfiguration, clickHouseDb *sql.DB) PhoneHomeAgent {

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
		clickHouseQueryTimes:      newDurationMeasurement(ctx),
		clickHouseInsertsTimes:    newDurationMeasurement(ctx),
		elasticReadTimes:          newDurationMeasurement(ctx),
		elasticWriteTimes:         newDurationMeasurement(ctx),
		elasticBypassedReadTimes:  newDurationMeasurement(ctx),
		elasticBypassedWriteTimes: newDurationMeasurement(ctx),

		ingestCounters:    NewMultiCounter(ctx, nil),
		userAgentCounters: NewMultiCounter(ctx, processUserAgent),
		telemetryEndpoint: configuration.QuesmaInternalTelemetryUrl,
		httpClient:        &http.Client{Timeout: time.Minute},
	}
}

func (a *agent) ClickHouseQueryDuration() DurationMeasurement {
	return a.clickHouseQueryTimes
}

func (a *agent) ClickHouseInsertDuration() DurationMeasurement {
	return a.clickHouseInsertsTimes
}

func (a *agent) ElasticReadRequestsDuration() DurationMeasurement {
	return a.elasticReadTimes
}

func (a *agent) ElasticWriteRequestsDuration() DurationMeasurement {
	return a.elasticWriteTimes
}

func (a *agent) ElasticBypassedReadRequestsDuration() DurationMeasurement {
	return a.elasticBypassedReadTimes
}

func (a *agent) ElasticBypassedWriteRequestsDuration() DurationMeasurement {
	return a.elasticBypassedWriteTimes
}

func (a *agent) IngestCounters() MultiCounter {
	return a.ingestCounters
}

func (a *agent) UserAgentCounters() MultiCounter {
	return a.userAgentCounters
}

func (a *agent) RecentStats() (recent PhoneHomeStats, available bool) {
	return a.recent, a.recent.TakenAt != 0
}

func (a *agent) collectClickHouseUsage(ctx context.Context, stats *ClickHouseStats) {
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

	rows, err := a.clickHouseDb.QueryContext(ctx, totalSummaryQuery)

	if err != nil {
		logger.WarnWithCtxAndReason(ctx, "No clickhouse stats").Err(err).Msg("Error getting stats from clickhouse.")
		return
	}

	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&stats.NumberOfRows, &stats.DiskSpace)
		if err != nil {
			logger.WarnWithCtxAndReason(ctx, "No clickhouse stats").Err(err).Msg("Error getting stats from clickhouse.")
			return
		}
	}

	if rows.Err() != nil {
		logger.WarnWithCtxAndReason(ctx, "No clickhouse stats").Err(rows.Err()).Msg("Error getting stats from clickhouse.")
	}
}

func (a *agent) collectClickHouseVersion(ctx context.Context, stats *ClickHouseStats) {

	// https://clickhouse.com/docs/en/sql-reference/functions/other-functions#version
	totalSummaryQuery := `select version()`

	ctx, cancel := context.WithTimeout(ctx, clickhouseTimeout)
	defer cancel()

	rows, err := a.clickHouseDb.QueryContext(ctx, totalSummaryQuery)

	if err != nil {
		logger.Error().Err(err).Msg("Error getting version from clickhouse.")
		return
	}

	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&stats.ServerVersion)
		if err != nil {
			logger.Error().Err(err).Msg("Error getting version from clickhouse.")
			return
		}
	}

	if rows.Err() != nil {
		logger.Error().Err(rows.Err()).Msg("Error getting version from clickhouse.")
	}
}

func (a *agent) CollectClickHouse(ctx context.Context) (stats ClickHouseStats) {

	// https://gist.github.com/sanchezzzhak/511fd140e8809857f8f1d84ddb937015
	stats.Status = statusNotOk

	dbStats := a.clickHouseDb.Stats()

	stats.MaxOpenConnection = dbStats.MaxOpenConnections
	stats.OpenConnection = dbStats.OpenConnections

	a.collectClickHouseUsage(ctx, &stats)
	a.collectClickHouseVersion(ctx, &stats)

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
		logger.Error().Err(err).Msg("Error getting stats from elasticsearch. ")
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

func (a *agent) collectElasticUsage(ctx context.Context, stats *ElasticStats) (err error) {
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

func (a *agent) collectElasticVersion(ctx context.Context, stats *ElasticStats) (err error) {

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

func (a *agent) CollectElastic(ctx context.Context) (stats ElasticStats) {

	stats.Status = statusNotOk

	err := a.collectElasticVersion(ctx, &stats)
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
	if a.config.Elasticsearch.User != "" {
		req.SetBasicAuth(a.config.Elasticsearch.User, a.config.Elasticsearch.Password)
	}
	return req, nil
}

func (a *agent) runtimeStats() (stats RuntimeStats) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	stats.MemoryUsed = m.Alloc
	if v, errV := mem.VirtualMemory(); errV == nil {
		stats.MemoryAvailable = v.Total
	}

	return stats
}

func (a *agent) collect(ctx context.Context, reportType string) (stats PhoneHomeStats) {

	stats.ConfigMode = a.config.Mode.String()
	stats.ReportType = reportType
	stats.Hostname = a.hostname
	stats.AgentStartedAt = a.statedAt.Unix()
	stats.TakenAt = time.Now().Unix()
	stats.QuesmaVersion = buildinfo.Version
	stats.NumberOfPanics = recovery.PanicCounter.Load()
	stats.InstanceID = a.instanceId

	stats.ClickHouse = a.CollectClickHouse(ctx)
	stats.Elasticsearch = a.CollectElastic(ctx)

	stats.ClickHouseQueriesDuration = a.ClickHouseQueryDuration().Aggregate()
	stats.ClickHouseInsertsDuration = a.ClickHouseInsertDuration().Aggregate()
	stats.ElasticReadsDuration = a.ElasticReadRequestsDuration().Aggregate()
	stats.ElasticWritesDuration = a.ElasticWriteRequestsDuration().Aggregate()
	stats.ElasticBypassedReadsDuration = a.ElasticBypassedReadRequestsDuration().Aggregate()
	stats.ElasticBypassedWritesDuration = a.ElasticBypassedWriteRequestsDuration().Aggregate()
	stats.UserAgentCounters = a.userAgentCounters.AggregateTopValues()

	stats.IngestCounters = a.ingestCounters.Aggregate()

	stats.RuntimeStats = a.runtimeStats()
	stats.TopErrors = a.topErrors()

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
	request.Header.Set(config.LicenseHeader, a.config.LicenseKey)

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
	request.Header.Set(config.LicenseHeader, a.config.LicenseKey)

	resp, err := a.httpClient.Do(request)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("phone home failed, invalid status code: %v", resp.StatusCode)
	}

	return nil
}

func (a *agent) report(ctx context.Context, stats PhoneHomeStats) {

	data, err := json.Marshal(stats)
	if err != nil {
		logger.Error().Err(err).Msgf("Error marshalling stats")
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

	// do not collect stats immediately
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

	// collect the last stats using given context
	a.telemetryCollection(ctx, reportTypeOnShutdown)

	// stop all

	logger.Info().Msg("PhoneHomeAgent Stopped")

}
