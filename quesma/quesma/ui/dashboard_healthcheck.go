// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"github.com/QuesmaOrg/quesma/quesma/end_user_errors"
	"github.com/QuesmaOrg/quesma/quesma/health"
	"sync"
	"time"
)

// Checking status by pinging is expensive, we don't want to do it too often.
// The cache is used to ping it at most once every 5 seconds.
type healthCheckStatusCache struct {
	mutex      sync.Mutex
	lastRun    time.Time
	scheduled  bool
	lastStatus health.Status
}

const healthCheckInterval = 5 * time.Second

func (c *healthCheckStatusCache) check(updateFunc func() health.Status) health.Status {
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
		lastStatus: health.NewStatus("grey", "N/A", "Have not run yet"),
		scheduled:  false,
		lastRun:    time.Unix(0, 0),
	}
}

func (qmc *QuesmaManagementConsole) checkClickhouseHealth() health.Status {
	if !qmc.cfg.WritesToClickhouse() && !qmc.cfg.ReadsFromClickhouse() {
		return health.NewStatus("grey", "N/A (not writing)", "")
	}

	return qmc.clickhouseStatusCache.check(func() health.Status {
		err := qmc.logManager.Ping()
		if err != nil {
			endUserError := end_user_errors.GuessClickhouseErrorType(err)
			return health.NewStatus("red", "Ping failed", endUserError.Reason())

		}
		return health.NewStatus("green", "Healthy", "")
	})
}

func (qmc *QuesmaManagementConsole) checkElasticsearch() health.Status {

	if !qmc.cfg.WritesToElasticsearch() && !qmc.cfg.ReadsFromElasticsearch() {
		return health.NewStatus("grey", "N/A (not writing)", "")
	}

	healthCheck := health.NewElasticHealthChecker(qmc.cfg)

	return qmc.elasticStatusCache.check(healthCheck.CheckHealth)
}

func (qmc *QuesmaManagementConsole) checkKibana() health.Status {
	statA := qmc.requestsStore.GetRequestsStats(RequestStatisticKibana2Clickhouse)
	statB := qmc.requestsStore.GetRequestsStats(RequestStatisticKibana2Elasticsearch)
	if statA.RatePerMinute > 0 || statB.RatePerMinute > 0 {
		return health.NewStatus("green", "Healthy", "We see requests from Kibana")
	} else {
		return health.NewStatus("grey", "N/A", "No requests from Kibana")
	}
}

func (qmc *QuesmaManagementConsole) checkIngest() health.Status {
	statA := qmc.requestsStore.GetRequestsStats(RequestStatisticIngest2Clickhouse)
	statB := qmc.requestsStore.GetRequestsStats(RequestStatisticIngest2Elasticsearch)
	if statA.RatePerMinute > 0 || statB.RatePerMinute > 0 {
		return health.NewStatus("green", "Healthy", "We see ingest traffic")
	} else {
		return health.NewStatus("grey", "N/A", "No ingest traffic")
	}
}
