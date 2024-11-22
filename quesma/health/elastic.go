// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package health

import (
	"fmt"
	"github.com/goccy/go-json"
	"io"
	"net/http"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/quesma/config"
	"strconv"
)

type ElasticHealthChecker struct {
	cfg        *config.QuesmaConfiguration
	httpClient *http.Client
}

func NewElasticHealthChecker(cfg *config.QuesmaConfiguration) Checker {
	return &ElasticHealthChecker{cfg: cfg, httpClient: &http.Client{}}
}

func (c *ElasticHealthChecker) checkIfElasticsearchDiskIsFull() (isFull bool, reason string) {
	const catAllocationPath = "/_cat/allocation?format=json"
	const maxDiskPercent = 90

	req, err := http.NewRequest(http.MethodGet, c.cfg.Elasticsearch.Url.String()+catAllocationPath, nil)
	if err != nil {
		logger.Error().Err(err).Msgf("Can't create '%s' request", catAllocationPath)
	}
	req = elasticsearch.AddBasicAuthIfNeeded(req, c.cfg.Elasticsearch.User, c.cfg.Elasticsearch.Password)
	resp, err := c.httpClient.Do(req)
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

func (c *ElasticHealthChecker) CheckHealth() Status {
	const elasticsearchHealthPath = "/_cluster/health/*"

	req, err := http.NewRequest(http.MethodGet, c.cfg.Elasticsearch.Url.String()+elasticsearchHealthPath, nil)
	if err != nil {
		return NewStatus("red", fmt.Sprintf("Can't create '%s' request", elasticsearchHealthPath), err.Error())
	}
	req = elasticsearch.AddBasicAuthIfNeeded(req, c.cfg.Elasticsearch.User, c.cfg.Elasticsearch.Password)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return NewStatus("red", "Ping failed", err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return NewStatus("red",
			fmt.Sprintf("Can't read '%s' response", elasticsearchHealthPath), err.Error())
	}
	var parsed map[string]interface{}
	err = json.Unmarshal(body, &parsed)
	if err != nil {
		return NewStatus("red",
			fmt.Sprintf("Can't parse json '%s' response", elasticsearchHealthPath), err.Error()+" "+string(body))
	}
	if parsed["status"] == "red" {
		message := "Cluster status is red"
		if isFull, addMsg := c.checkIfElasticsearchDiskIsFull(); isFull {
			message += ", " + addMsg
		}
		return NewStatus("red", message, string(body))
	}
	if resp.StatusCode == 200 {
		return NewStatus("green", "Healthy", "")
	} else {
		return NewStatus("red", "Failed", resp.Status)
	}
}
