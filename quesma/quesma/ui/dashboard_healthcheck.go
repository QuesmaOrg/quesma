// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/end_user_errors"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"sync"
	"time"
)

// Checking status by pinging is expensive, we don't want to do it too often.
// The cache is used to ping it at most once every 5 seconds.
type healthCheckStatusCache struct {
	mutex      sync.Mutex
	lastRun    time.Time
	scheduled  bool
	lastStatus quesma_api.Status
}

const healthCheckInterval = 5 * time.Second

func (c *healthCheckStatusCache) check(updateFunc func() quesma_api.Status) quesma_api.Status {
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
		lastStatus: quesma_api.NewStatus("grey", "N/A", "Have not run yet"),
		scheduled:  false,
		lastRun:    time.Unix(0, 0),
	}
}

func (qmc *QuesmaManagementConsole) checkClickhouseHealth() quesma_api.Status {
	if !qmc.cfg.WritesToClickhouse() && !qmc.cfg.ReadsFromClickhouse() {
		return quesma_api.NewStatus("grey", "N/A (not writing)", "")
	}

	return qmc.clickhouseStatusCache.check(func() quesma_api.Status {
		err := qmc.logManager.Ping()
		if err != nil {
			endUserError := end_user_errors.GuessClickhouseErrorType(err)
			return quesma_api.NewStatus("red", "Ping failed", endUserError.Reason())

		}
		return quesma_api.NewStatus("green", "Healthy", "")
	})
}

func (qmc *QuesmaManagementConsole) checkElasticsearch() quesma_api.Status {

	if !qmc.cfg.WritesToElasticsearch() && !qmc.cfg.ReadsFromElasticsearch() {
		return quesma_api.NewStatus("grey", "N/A (not writing)", "")
	}

	healthCheck := elasticsearch.NewElasticHealthChecker(qmc.cfg)

	return qmc.elasticStatusCache.check(healthCheck.CheckHealth)
}

func (qmc *QuesmaManagementConsole) checkKibana() quesma_api.Status {
	statA := qmc.requestsStore.GetRequestsStats(RequestStatisticKibana2Clickhouse)
	statB := qmc.requestsStore.GetRequestsStats(RequestStatisticKibana2Elasticsearch)
	if statA.RatePerMinute > 0 || statB.RatePerMinute > 0 {
		return quesma_api.NewStatus("green", "Healthy", "We see requests from Kibana")
	} else {
		return quesma_api.NewStatus("grey", "N/A", "No requests from Kibana")
	}
}

func (qmc *QuesmaManagementConsole) checkIngest() quesma_api.Status {
	statA := qmc.requestsStore.GetRequestsStats(RequestStatisticIngest2Clickhouse)
	statB := qmc.requestsStore.GetRequestsStats(RequestStatisticIngest2Elasticsearch)
	if statA.RatePerMinute > 0 || statB.RatePerMinute > 0 {
		return quesma_api.NewStatus("green", "Healthy", "We see ingest traffic")
	} else {
		return quesma_api.NewStatus("grey", "N/A", "No ingest traffic")
	}
}
