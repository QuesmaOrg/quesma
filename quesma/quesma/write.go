package quesma

import (
	"context"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/stats/errorstats"
	"mitmproxy/quesma/telemetry"
	"sync/atomic"
)

const IndexName = "_index"

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

func dualWriteBulk(ctx context.Context, defaultIndex *string, jsons mux.NDJSON, lm *clickhouse.LogManager,
	cfg config.QuesmaConfiguration, phoneHomeAgent telemetry.PhoneHomeAgent) (results []WriteResult) {
	if config.TrafficAnalysis.Load() {
		logger.Info().Msg("analysing traffic, not writing to Clickhouse")
		return
	}
	defer recovery.LogPanic()

	indicesWithDocumentsToInsert := make(map[string][]mux.JSON, len(jsons))
	for i := 0; i+1 < len(jsons); i += 2 {
		operation := jsons[i]  // {"create":{"_index":"kibana_sample_data_flights", "_id": 1}}
		document := jsons[i+1] // {"FlightNum":"9HY9SWR","DestCountry":"AU","OriginWeather":"Sunny","OriginCityName":"Frankfurt am Main" }

		var operationParsed map[string]DocumentTarget // operationName (create, index, update, delete) -> DocumentTarget

		err := operation.Remarshal(&operationParsed)
		if err != nil {
			logger.ErrorWithCtx(ctx).Err(err).Msgf("Invalid operation in _bulk: %s", operation)
			return nil
		}

		index := getTargetIndex(operationParsed, defaultIndex)
		if index == "" {
			logger.ErrorWithCtxAndReason(ctx, "no index name in _bulk").
				Msgf("Invalid index name in _bulk: %s", operation)
			continue
		}

		indexConfig, found := cfg.IndexConfig[index]
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
				fmt.Sprintf("Unexpected operation in _bulk: %v", operation))
			logger.Error().Msgf("Invalid JSON with operation definition in _bulk: %s", operation)
		}
	}
	for indexName, documents := range indicesWithDocumentsToInsert {
		phoneHomeAgent.IngestCounters().Add(indexName, int64(len(documents)))

		withConfiguration(ctx, cfg, indexName, make(mux.JSON), func() error {
			for _, document := range documents {
				stats.GlobalStatistics.Process(cfg, indexName, document, clickhouse.NestedSeparator)
			}
			return lm.ProcessInsertQuery(ctx, indexName, documents)
		})
	}
	return results
}

func getTargetIndex(operation map[string]DocumentTarget, defaultIndex *string) string {
	for _, target := range operation { // this map contains only 1 element though
		if target.Index != nil {
			return *target.Index
		}
	}
	if defaultIndex != nil {
		return *defaultIndex
	}
	return ""
}

func dualWrite(ctx context.Context, tableName string, body mux.JSON, lm *clickhouse.LogManager, cfg config.QuesmaConfiguration) {
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
		return lm.ProcessInsertQuery(ctx, tableName, mux.NDJSON{body})
	})
}

var insertCounter = atomic.Int32{}

func withConfiguration(ctx context.Context, cfg config.QuesmaConfiguration, indexName string, body mux.JSON, action func() error) {
	if len(cfg.IndexConfig) == 0 {
		logger.InfoWithCtx(ctx).Msgf("%s  --> clickhouse, body(shortened): %s", indexName, body.ShortString())
		err := action()
		if err != nil {
			logger.ErrorWithCtx(ctx).Msg("Can't write to index: " + err.Error())
		}
	} else {
		matchingConfig, ok := findMatchingConfig(indexName, cfg)
		if !ok {
			logger.InfoWithCtx(ctx).Msgf("index '%s' is not configured, skipping", indexName)
			return
		}
		if matchingConfig.Enabled {
			insertCounter.Add(1)
			if insertCounter.Load()%50 == 1 {
				logger.DebugWithCtx(ctx).Msgf("%s  --> clickhouse, body(shortened): %s, ctr: %d", indexName, body.ShortString(), insertCounter.Load())
			}
			err := action()
			if err != nil {
				logger.ErrorWithCtx(ctx).Msg("Can't write to Clickhouse: " + err.Error())
			}
		} else {
			logger.InfoWithCtx(ctx).Msgf("index '%s' is disabled, ignoring", indexName)
		}
	}
}

var matchCounter = atomic.Int32{}

func findMatchingConfig(indexPattern string, cfg config.QuesmaConfiguration) (config.IndexConfiguration, bool) {
	matchCounter.Add(1)
	for _, indexConfig := range cfg.IndexConfig {
		if matchCounter.Load()%100 == 1 {
			logger.Debug().Msgf("matching index %s with config: %+v, ctr: %d", indexPattern, indexConfig.Name, matchCounter.Load())
		}
		if config.MatchName(indexPattern, indexConfig.Name) {
			if matchCounter.Load()%100 == 1 {
				logger.Debug().Msgf("  ╚═ matched index %s with config: %+v, ctr: %d", indexPattern, indexConfig.Name, matchCounter.Load())
			}
			return indexConfig, true
		} else {
			if matchCounter.Load()%100 == 1 {
				logger.Debug().Msgf("  ╚═ not matched index %s with config: %+v, ctr: %d", indexPattern, indexConfig.Name, matchCounter.Load())
			}
		}
	}
	return config.IndexConfiguration{}, false
}
