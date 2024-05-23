package ui

import (
	"mitmproxy/quesma/end_user_errors"
	"net/http"
	"sync"
	"time"
)

// Checking status by pinging is expensive, we don't want to do it too often.
// The cache is used to ping it at most once every 5 seconds.
type healthCheckStatusCache struct {
	mutex      sync.Mutex
	lastRun    time.Time
	scheduled  bool
	lastStatus healthCheckStatus
}

const healthCheckInterval = 5 * time.Second

func (c *healthCheckStatusCache) check(updateFunc func() healthCheckStatus) healthCheckStatus {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.scheduled || time.Since(c.lastRun) > healthCheckInterval {
		c.scheduled = true
		go func() {
			status := updateFunc()
			c.mutex.Lock()
			defer c.mutex.Unlock()
			c.lastStatus = status
			c.lastRun = time.Now()
			c.scheduled = false
		}()
	}
	return c.lastStatus
}

func newHealthCheckStatusCache() healthCheckStatusCache {
	return healthCheckStatusCache{
		lastStatus: healthCheckStatus{"grey", "N/A", "Have not run yet"},
		scheduled:  false,
		lastRun:    time.Unix(0, 0),
	}
}

type healthCheckStatus struct {
	status  string
	message string
	tooltip string
}

func (qmc *QuesmaManagementConsole) checkClickhouseHealth() healthCheckStatus {
	if !qmc.config.WritesToClickhouse() && !qmc.config.ReadsFromClickhouse() {
		return healthCheckStatus{"grey", "N/A (not writing)", ""}
	}

	return qmc.clickhouseStatusCache.check(func() healthCheckStatus {
		err := qmc.logManager.Ping()
		if err != nil {
			endUserError := end_user_errors.GuessClickhouseError(err)
			return healthCheckStatus{"red", "Ping failed", endUserError.Reason()}

		}
		return healthCheckStatus{"green", "Healthy", ""}
	})
}

func (qmc *QuesmaManagementConsole) checkElasticsearch() healthCheckStatus {
	if !qmc.config.WritesToElasticsearch() && !qmc.config.ReadsFromElasticsearch() {
		return healthCheckStatus{"grey", "N/A (not writing)", ""}
	}

	return qmc.elasticStatusCache.check(func() healthCheckStatus {
		resp, err := http.Get(qmc.config.Elasticsearch.Url.String())
		if err != nil {
			return healthCheckStatus{"red", "Ping failed", err.Error()}
		}
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			return healthCheckStatus{"green", "Healthy", ""}
		} else {
			return healthCheckStatus{"red", "Failed", resp.Status}
		}
	})
}

func (qmc *QuesmaManagementConsole) checkKibana() healthCheckStatus {
	statA := qmc.requestsStore.GetRequestsStats(RequestStatisticKibana2Clickhouse)
	statB := qmc.requestsStore.GetRequestsStats(RequestStatisticKibana2Elasticsearch)
	if statA.RatePerMinute > 0 || statB.RatePerMinute > 0 {
		return healthCheckStatus{"green", "Healthy", "We see requests from Kibana"}
	} else {
		return healthCheckStatus{"grey", "N/A", "No requests from Kibana"}
	}
}

func (qmc *QuesmaManagementConsole) checkIngest() healthCheckStatus {
	statA := qmc.requestsStore.GetRequestsStats(RequestStatisticIngest2Clickhouse)
	statB := qmc.requestsStore.GetRequestsStats(RequestStatisticIngest2Elasticsearch)
	if statA.RatePerMinute > 0 || statB.RatePerMinute > 0 {
		return healthCheckStatus{"green", "Healthy", "We see ingest traffic"}
	} else {
		return healthCheckStatus{"grey", "N/A", "No ingest traffic"}
	}
}
