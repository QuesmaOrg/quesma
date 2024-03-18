package telemetry

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"mitmproxy/quesma/buildinfo"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/recovery"
	"net/http"
	"net/url"
	"time"
)

const (
	warmupInterval    = 30 * time.Second
	phoneHomeInterval = 3600 * time.Second

	clickhouseTimeout = 10 * time.Second
	elasticTimeout    = 10 * time.Second

	statusOk    = "ok"
	statusNotOk = "not-ok"
)

type ClickHouseStats struct {
	Status            string `json:"status"`
	NumberOfRows      int64  `json:"number_of_rows" db:"number_of_rows"`
	DiskSpace         int64  `json:"disk_space"`
	OpenConnection    int    `json:"open_connection"`
	MaxOpenConnection int    `json:"max_open_connection"`
}

type ElasticStats struct {
	Status       string `json:"status"`
	NumberOfDocs int64  `json:"number_of_docs"`
	Size         int64  `json:"size"`
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

	TakenAt int64 `json:"taken_at"`
}

type PhoneHomeAgent interface {
	Start()
	Stop()

	RecentStats() (recent PhoneHomeStats, available bool)

	ClickHouseQueryDuration() DurationMeasurement
}

type agent struct {
	ctx    context.Context
	cancel context.CancelFunc

	clickHouseDb *sql.DB
	config       config.QuesmaConfiguration

	instanceId string
	statedAt   time.Time
	hostname   string

	clickHouseQueryTimes DurationMeasurement

	recent PhoneHomeStats
}

func NewPhoneHomeAgent(configuration config.QuesmaConfiguration, clickHouseDb *sql.DB) PhoneHomeAgent {

	// TODO
	// this is a question, maybe we should inherit context from the caller
	// maybe the main function should be the one to cancel the context

	ctx, cancel := context.WithCancel(context.Background())

	return &agent{
		ctx:                  ctx,
		cancel:               cancel,
		hostname:             "localhost", // FIXME
		instanceId:           "unknown",   // FIXME
		clickHouseDb:         clickHouseDb,
		config:               configuration,
		clickHouseQueryTimes: newDurationMeasurement(ctx),
	}
}

func (a *agent) ClickHouseQueryDuration() DurationMeasurement {
	return a.clickHouseQueryTimes
}

func (a *agent) RecentStats() (recent PhoneHomeStats, available bool) {
	return a.recent, a.recent.TakenAt != 0
}

func (a *agent) CollectClickHouse() (stats ClickHouseStats) {

	// https://gist.github.com/sanchezzzhak/511fd140e8809857f8f1d84ddb937015
	stats.Status = statusNotOk

	dbStats := a.clickHouseDb.Stats()

	stats.MaxOpenConnection = dbStats.MaxOpenConnections
	stats.OpenConnection = dbStats.OpenConnections

	// it counts whole clickhouse database, including system tables
	totalSummaryQuery := `
select 
       sum(rows) as rows,
       sum(bytes) as bytes_size
from system.parts
where active

`
	ctx, cancel := context.WithTimeout(a.ctx, clickhouseTimeout)
	defer cancel()

	rows, err := a.clickHouseDb.QueryContext(ctx, totalSummaryQuery)

	if err != nil {
		logger.Error().Err(err).Msg("Error getting stats from clickhouse.")
		return stats
	}

	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&stats.NumberOfRows, &stats.DiskSpace)
		if err != nil {
			logger.Error().Err(err).Msg("Error getting stats from clickhouse.")
			return stats
		}
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

func scanElasticResponse(body []byte, stats *ElasticStats) error {

	response := elasticStatsResponse{}

	err := json.Unmarshal(body, &response)
	if err != nil {
		logger.Error().Err(err).Msg("Error getting stats from elasticsearch. JSON parsing failed.")
		return err
	}

	stats.NumberOfDocs = response.All.Total.Docs.Count
	stats.Size = response.All.Total.Store.SizeInBytes

	return nil
}

func (a *agent) CollectElastic() (stats ElasticStats) {

	stats.Status = statusNotOk
	// https://www.datadoghq.com/blog/collect-elasticsearch-metrics/

	// queries
	//curl  -s 'http://localhost:9200/_all/_stats?pretty=true' | jq ._all.total.docs
	//curl  -s 'http://localhost:9200/_all/_stats?pretty=true' | jq ._all.total.store

	elasticUrl := a.config.ElasticsearchUrl

	statsUrl := elasticUrl.JoinPath("/_all/_stats")

	ctx, cancel := context.WithTimeout(a.ctx, elasticTimeout)
	defer cancel()

	request, err := a.buildElastisearchRequest(ctx, statsUrl, nil)

	if err != nil {
		logger.Error().Err(err).Msg("Error getting stats from elasticsearch. ")
		return stats
	}

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		logger.Error().Err(err).Msg("Error getting stats from elasticsearch. ")
		return stats
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error().Msgf("Error getting stats from elasticsearch. URL %s got status code: %v", statsUrl.String(), resp.StatusCode)
		return stats
	}

	body, err := io.ReadAll(resp.Body)

	if err != nil {
		logger.Error().Err(err).Msg("Error getting stats from elasticsearch. Reading the body failed")
		return stats
	}

	if err = scanElasticResponse(body, &stats); err != nil {
		logger.Error().Err(err).Msg("Error getting stats from elasticsearch. JSON parsing failed.")
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
	if a.config.ElasticsearchUser != "" {
		req.SetBasicAuth(a.config.ElasticsearchUser, a.config.ElasticsearchPassword)
	}
	return req, nil
}

func (a agent) collect() (stats PhoneHomeStats) {

	stats.Hostname = a.hostname
	stats.AgentStartedAt = a.statedAt.Unix()
	stats.TakenAt = time.Now().Unix()
	stats.QuesmaVersion = buildinfo.Version
	stats.InstanceID = a.instanceId

	stats.ClickHouse = a.CollectClickHouse()
	stats.Elasticsearch = a.CollectElastic()

	stats.ClickHouseQueriesDuration = a.ClickHouseQueryDuration().Aggregate()

	return stats
}

func (a *agent) report(stats PhoneHomeStats) {

	data, err := json.Marshal(stats)
	if err != nil {
		logger.Error().Err(err).Msgf("Error marshalling stats")
		return
	}
	logger.Info().Msgf("Call Home: %v", string(data))
}

func (a *agent) telemetryCollection() {

	// if we fail we would not die
	defer recovery.LogPanic()

	stats := a.collect()

	a.report(stats)

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

		a.telemetryCollection()

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

func (a *agent) Stop() {

	a.cancel()
	logger.Info().Msg("PhoneHomeAgent Stopped")
}
