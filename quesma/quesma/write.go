package quesma

import (
	"context"
	"encoding/json"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/util"
	"regexp"
	"strings"
	"sync/atomic"
)

func dualWriteBulk(ctx context.Context, optionalTableName string, body string, lm *clickhouse.LogManager, cfg config.QuesmaConfiguration) {
	_ = ctx
	if config.TrafficAnalysis.Load() {
		logger.Info().Msgf("analysing traffic, not writing to Clickhouse %s", optionalTableName)
		return
	}
	defer recovery.LogPanic()
	jsons := strings.Split(body, "\n")
	for i := 0; i+1 < len(jsons); i += 2 {
		action := jsons[i]
		document := jsons[i+1]

		var jsonData map[string]interface{}

		// Unmarshal the JSON data into the map
		err := json.Unmarshal([]byte(action), &jsonData)
		if err != nil {
			logger.Info().Msgf("Invalid action JSON in _bulk: %v %s", err, action)
			continue
		}
		if jsonData["create"] != nil {
			createObj, ok := jsonData["create"].(map[string]interface{})
			if !ok {
				logger.Info().Msgf("Invalid create JSON in _bulk: %s", action)
				continue
			}
			indexName, ok := createObj["_index"].(string)
			if !ok {
				if len(indexName) == 0 {
					logger.Error().Msgf("Invalid create JSON in _bulk, no _index name: %s", action)
					continue
				} else {
					indexName = optionalTableName
				}
			}

			withConfiguration(ctx, cfg, indexName, document, func() error {
				stats.GlobalStatistics.Process(indexName, document, clickhouse.NestedSeparator)
				return lm.ProcessInsertQuery(indexName, document)
			})
		} else if jsonData["index"] != nil {
			logger.Warn().Msg("Not supporting 'index' _bulk.")
		} else if jsonData["update"] != nil {
			logger.Warn().Msg("Not supporting 'update' _bulk.")
		} else if jsonData["delete"] != nil {
			logger.Warn().Msg("Not supporting 'delete' _bulk.")
		} else {
			logger.Error().Msgf("Invalid action JSON in _bulk: %s", action)
		}
	}
}

func dualWrite(ctx context.Context, tableName string, body string, lm *clickhouse.LogManager, cfg config.QuesmaConfiguration) {
	_ = ctx
	stats.GlobalStatistics.Process(tableName, body, clickhouse.NestedSeparator)
	if config.TrafficAnalysis.Load() {
		logger.Info().Msgf("analysing traffic, not writing to Clickhouse %s", tableName)
		return
	}

	defer recovery.LogPanic()
	if len(body) == 0 {
		return
	}

	withConfiguration(ctx, cfg, tableName, body, func() error {
		return lm.ProcessInsertQuery(tableName, body)
	})
}

var insertCounter = atomic.Int32{}

func withConfiguration(ctx context.Context, cfg config.QuesmaConfiguration, indexName string, body string, action func() error) {
	if len(cfg.IndexConfig) == 0 {
		logger.Info().Msgf("%s  --> clickhouse, body(shortened): %s", indexName, util.Truncate(body))
		err := action()
		if err != nil {
			logger.Fatal().Msg(err.Error())
		}
	} else {
		matchingConfig, ok := findMatchingConfig(indexName, cfg)
		if !ok {
			logger.Info().Msgf("index '%s' is not configured, skipping", indexName)
			return
		}
		if matchingConfig.Enabled {
			insertCounter.Add(1)
			if insertCounter.Load()%50 == 1 {
				logger.Debug().Msgf("%s  --> clickhouse, body(shortened): %s, ctr: %d", indexName, util.Truncate(body), insertCounter.Load())
			}
			err := action()
			if err != nil {
				logger.Fatal().Msg(err.Error())
			}
		} else {
			logger.Info().Msgf("index '%s' is disabled, ignoring", indexName)
		}
	}
}

func matches(indexName string, indexNamePattern string) bool {
	r, err := regexp.Compile(strings.Replace(indexNamePattern, "*", ".*", -1))
	if err != nil {
		logger.Error().Msgf("invalid index name pattern [%s]: %s", indexNamePattern, err)
		return false
	}

	return r.MatchString(indexName)
}

var matchCounter = atomic.Int32{}

func findMatchingConfig(indexName string, cfg config.QuesmaConfiguration) (config.IndexConfiguration, bool) {
	matchCounter.Add(1)
	for _, indexConfig := range cfg.IndexConfig {
		if matchCounter.Load()%100 == 1 {
			logger.Debug().Msgf("matching index %s with config: %+v, ctr: %d", indexName, indexConfig.NamePattern, matchCounter.Load())
		}
		if matches(indexName, indexConfig.NamePattern) {
			if matchCounter.Load()%100 == 1 {
				logger.Debug().Msgf("  ╚═ matched index %s with config: %+v, ctr: %d", indexName, indexConfig.NamePattern, matchCounter.Load())
			}
			return indexConfig, true
		} else {
			if matchCounter.Load()%100 == 1 {
				logger.Info().Msgf("  ╚═ not matched index %s with config: %+v, ctr: %d", indexName, indexConfig.NamePattern, matchCounter.Load())
			}
		}
	}
	return config.IndexConfiguration{}, false
}
