package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"mitmproxy/quesma/end_user_errors"
	"mitmproxy/quesma/logger"
	"net/http"
	"strconv"
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
	if !qmc.cfg.WritesToClickhouse() && !qmc.cfg.ReadsFromClickhouse() {
		return healthCheckStatus{"grey", "N/A (not writing)", ""}
	}

	return qmc.clickhouseStatusCache.check(func() healthCheckStatus {
		err := qmc.logManager.Ping()
		if err != nil {
			endUserError := end_user_errors.GuessClickhouseErrorType(err)
			return healthCheckStatus{"red", "Ping failed", endUserError.Reason()}

		}
		return healthCheckStatus{"green", "Healthy", ""}
	})
}

func (qmc *QuesmaManagementConsole) checkIfElasticsearchDiskIsFull() (isFull bool, reason string) {
	const catAllocationPath = "/_cat/allocation?format=json"
	const maxDiskPercent = 90

	resp, err := http.Get(qmc.cfg.Elasticsearch.Url.String() + catAllocationPath)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var parsed []map[string]interface{}
	err = json.Unmarshal(body, &parsed)
	if err != nil {
		logger.Error().Err(err).Msgf("Can't parse json '%s'' response", catAllocationPath)
		return
	}
	for _, shards := range parsed {
		if diskPercentRaw, exists := shards["disk.percent"]; exists && diskPercentRaw != nil {
			if diskPercentStr, isStr := diskPercentRaw.(string); isStr {
				if diskPercentInt, err := strconv.Atoi(diskPercentStr); err == nil {
					if diskPercentInt >= maxDiskPercent {
						return true, fmt.Sprintf("Not enough space on disk %d%% >= %d%%", diskPercentInt, maxDiskPercent)
					}
				} else {
					logger.Error().Msgf("Can't parse disk.percent as int '%s'", diskPercentStr)
				}
			} else {
				logger.Error().Msgf("Can't parse disk.percent as string, '%v'", diskPercentRaw)
			}
		} else {
			logger.Error().Msg("Can't find disk.percent in response")
		}
	}
	return
}

func (qmc *QuesmaManagementConsole) checkElasticsearch() healthCheckStatus {
	const elasticsearchHealthPath = "/_cluster/health/*"

	if !qmc.cfg.WritesToElasticsearch() && !qmc.cfg.ReadsFromElasticsearch() {
		return healthCheckStatus{"grey", "N/A (not writing)", ""}
	}

	return qmc.elasticStatusCache.check(func() healthCheckStatus {
		resp, err := http.Get(qmc.cfg.Elasticsearch.Url.String() + elasticsearchHealthPath)
		if err != nil {
			return healthCheckStatus{"red", "Ping failed", err.Error()}
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return healthCheckStatus{"red",
				fmt.Sprintf("Can't read '%s' response", elasticsearchHealthPath), err.Error()}
		}
		var parsed map[string]interface{}
		err = json.Unmarshal(body, &parsed)
		if err != nil {
			return healthCheckStatus{"red",
				fmt.Sprintf("Can't parse json '%s' response", elasticsearchHealthPath), err.Error() + " " + string(body)}
		}
		if parsed["status"] == "red" {
			message := "Cluster status is red"
			if isFull, addMsg := qmc.checkIfElasticsearchDiskIsFull(); isFull {
				message += ", " + addMsg
			}
			return healthCheckStatus{"red", message, string(body)}
		}
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
