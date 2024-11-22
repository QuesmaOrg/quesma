// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package health

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/quesma/config"
	"strconv"
)

type ElasticHealthChecker struct {
	cfg    *config.QuesmaConfiguration
	client *elasticsearch.SimpleClient
}

func NewElasticHealthChecker(cfg *config.QuesmaConfiguration) Checker {
	return &ElasticHealthChecker{cfg: cfg, client: elasticsearch.NewSimpleClient(&cfg.Elasticsearch)}
}

func (c *ElasticHealthChecker) checkIfElasticsearchDiskIsFull() (isFull bool, reason string) {
	const catAllocationPath = "_cat/allocation?format=json"
	const maxDiskPercent = 90

	resp, err := c.client.Request(context.Background(), http.MethodGet, catAllocationPath, nil)
	if err != nil {
		logger.Error().Err(err).Msgf("Failed calling %s", catAllocationPath)
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
	const elasticsearchHealthPath = "_cluster/health/*"
	resp, err := c.client.Request(context.Background(), http.MethodGet, elasticsearchHealthPath, nil)
	if err != nil {
		return NewStatus("red", fmt.Sprintf("Failed calling %s", elasticsearchHealthPath), err.Error())
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
