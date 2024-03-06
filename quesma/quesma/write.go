package quesma

import (
	"context"
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/stats/errorstats"
	"mitmproxy/quesma/util"
	"regexp"
	"strings"
	"sync/atomic"
)

type (
	DocumentTarget struct {
		Index *string `json:"_index"`
		Id    *string `json:"_id"` // document's target id in Elasticsearch, we ignore it when writing to Clickhouse.
	}
	WriteResult struct {
		Operation string
		Index     string
	}
)

func dualWriteBulk(ctx context.Context, body string, lm *clickhouse.LogManager, cfg config.QuesmaConfiguration) (results []WriteResult) {
	if config.TrafficAnalysis.Load() {
		logger.Info().Msg("analysing traffic, not writing to Clickhouse")
		return
	}
	defer recovery.LogPanic()
	jsons := strings.Split(body, "\n")

	indicesWithDocumentsToInsert := make(map[string][]string)
	for i := 0; i+1 < len(jsons); i += 2 {
		operationDef := jsons[i] // {"create":{"_index":"kibana_sample_data_flights", "_id": 1}}
		document := jsons[i+1]   // {"FlightNum":"9HY9SWR","DestCountry":"AU","OriginWeather":"Sunny","OriginCityName":"Frankfurt am Main" }

		var operation map[string]DocumentTarget // operationName (create, index, update, delete) -> DocumentTarget

		err := json.Unmarshal([]byte(operationDef), &operation)
		if err != nil || len(operation) != 1 {
			logger.Info().Msgf("Invalid action JSON in _bulk: %v %+v", err, operation)
			continue
		}
		index := getTargetIndex(operation)
		if index == "" {
			logger.ErrorWithCtxAndReason(ctx, "no index name in _bulk").
				Msgf("Invalid index name in _bulk: %s", operationDef)
			continue
		}

		indexConfig, found := cfg.GetIndexConfig(index)
		if !found {
			logger.Debug().Msgf("index '%s' is not configured, skipping", index)
			continue
		}
		if !indexConfig.Enabled {
			logger.Debug().Msgf("index '%s' is disabled, ignoring", index)
			continue
		}

		if _, ok := operation["create"]; ok {
			results = append(results, WriteResult{"create", index})
			indicesWithDocumentsToInsert[index] = append(indicesWithDocumentsToInsert[index], document)
		} else if _, ok = operation["index"]; ok {
			results = append(results, WriteResult{"index", index})
			indicesWithDocumentsToInsert[index] = append(indicesWithDocumentsToInsert[index], document)
		} else if _, ok = operation["update"]; ok {
			errorstats.GlobalErrorStatistics.RecordKnownError("_bulk update is not supported", nil,
				"We do not support 'update' in _bulk")
			logger.Debug().Msg("Not supporting 'update' _bulk.")
		} else if _, ok = operation["delete"]; ok {
			errorstats.GlobalErrorStatistics.RecordKnownError("_bulk delete is not supported", nil,
				"We do not support 'delete' in _bulk")
			logger.Debug().Msg("Not supporting 'delete' _bulk.")
		} else {
			errorstats.GlobalErrorStatistics.RecordUnknownError(nil,
				"Unexpected operation in _bulk: "+operationDef)
			logger.Error().Msgf("Invalid JSON with operation definition in _bulk: %s", operationDef)
		}
	}
	for indexName, documents := range indicesWithDocumentsToInsert {
		withConfiguration(ctx, cfg, indexName, "{ BULK_PAYLOAD }", func() error {
			for _, document := range documents {
				stats.GlobalStatistics.Process(cfg, indexName, document, clickhouse.NestedSeparator)
			}
			return lm.ProcessInsertQuery(indexName, documents)
		})
	}
	return results
}

func getTargetIndex(operation map[string]DocumentTarget) string {
	for _, target := range operation { // this map contains only 1 element though
		if target.Index != nil {
			return *target.Index
		}
	}
	return ""
}

func dualWrite(ctx context.Context, tableName string, body string, lm *clickhouse.LogManager, cfg config.QuesmaConfiguration) {
	stats.GlobalStatistics.Process(cfg, tableName, body, clickhouse.NestedSeparator)
	if config.TrafficAnalysis.Load() {
		logger.Info().Msgf("analysing traffic, not writing to Clickhouse %s", tableName)
		return
	}

	defer recovery.LogPanic()
	if len(body) == 0 {
		return
	}

	withConfiguration(ctx, cfg, tableName, body, func() error {
		return lm.ProcessInsertQuery(tableName, []string{body})
	})
}

var insertCounter = atomic.Int32{}

func withConfiguration(ctx context.Context, cfg config.QuesmaConfiguration, indexName string, body string, action func() error) {
	if len(cfg.IndexConfig) == 0 {
		logger.Info().Msgf("%s  --> clickhouse, body(shortened): %s", indexName, util.Truncate(body))
		err := action()
		if err != nil {
			logger.Fatal().Msg("Can't write to index: " + err.Error())
		}
	} else {
		matchingConfig, ok := findMatchingConfig(ctx, indexName, cfg)
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
				logger.ErrorWithCtx(ctx).Msg("Can't write to Clickhouse: " + err.Error())
			}
		} else {
			logger.Info().Msgf("index '%s' is disabled, ignoring", indexName)
		}
	}
}

func matches(ctx context.Context, indexName string, indexNamePattern string) bool {
	r, err := regexp.Compile(strings.Replace(indexNamePattern, "*", ".*", -1))
	if err != nil {
		msg := fmt.Sprintf("invalid index name pattern [%s]: %s", indexNamePattern, err)
		logger.ErrorWithCtxAndReason(ctx, "invalid index name pattern").Msg(msg)
		return false
	}

	return r.MatchString(indexName)
}

var matchCounter = atomic.Int32{}

func findMatchingConfig(ctx context.Context, indexName string, cfg config.QuesmaConfiguration) (config.IndexConfiguration, bool) {
	matchCounter.Add(1)
	for _, indexConfig := range cfg.IndexConfig {
		if matchCounter.Load()%100 == 1 {
			logger.Debug().Msgf("matching index %s with config: %+v, ctr: %d", indexName, indexConfig.NamePattern, matchCounter.Load())
		}
		if matches(ctx, indexName, indexConfig.NamePattern) {
			if matchCounter.Load()%100 == 1 {
				logger.Debug().Msgf("  ╚═ matched index %s with config: %+v, ctr: %d", indexName, indexConfig.NamePattern, matchCounter.Load())
			}
			return indexConfig, true
		} else {
			if matchCounter.Load()%100 == 1 {
				logger.Debug().Msgf("  ╚═ not matched index %s with config: %+v, ctr: %d", indexName, indexConfig.NamePattern, matchCounter.Load())
			}
		}
	}
	return config.IndexConfiguration{}, false
}
