package bulk

import (
	"context"
	"fmt"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/stats"
	"quesma/stats/errorstats"
	"quesma/telemetry"
	"sync/atomic"
)

type (
	WriteResult struct {
		Operation string
		Index     string
	}
)

func Write(ctx context.Context, defaultIndex *string, bulk types.NDJSON, lm *clickhouse.LogManager,
	cfg config.QuesmaConfiguration, phoneHomeAgent telemetry.PhoneHomeAgent) (results []WriteResult) {
	defer recovery.LogPanic()

	indicesWithDocumentsToInsert := make(map[string][]types.JSON, len(bulk))

	err := bulk.BulkForEach(func(op types.BulkOperation, document types.JSON) {

		index := op.GetIndex()
		operation := op.GetOperation()

		if index == "" {
			if defaultIndex != nil {
				index = *defaultIndex
			} else {
				logger.ErrorWithCtxAndReason(ctx, "no index name in _bulk").
					Msgf("Invalid index name in _bulk: %s", operation)
				return
			}
		}

		indexConfig, found := cfg.IndexConfig[index]
		if !found {
			logger.Debug().Msgf("index '%s' is not configured, skipping", index)
			return
		}
		if !indexConfig.Enabled {
			logger.Debug().Msgf("index '%s' is disabled, ignoring", index)
			return
		}

		switch operation {
		case "create", "index":
			results = append(results, WriteResult{operation, index})
			indicesWithDocumentsToInsert[index] = append(indicesWithDocumentsToInsert[index], document)
		case "update":

			errorstats.GlobalErrorStatistics.RecordKnownError("_bulk update is not supported", nil,
				"We do not support 'update' in _bulk")
			logger.Debug().Msg("Not supporting 'update' _bulk.")

		case "delete":
			errorstats.GlobalErrorStatistics.RecordKnownError("_bulk delete is not supported", nil,
				"We do not support 'delete' in _bulk")
			logger.Debug().Msg("Not supporting 'delete' _bulk.")

		default:
			errorstats.GlobalErrorStatistics.RecordUnknownError(nil,
				fmt.Sprintf("Unexpected operation in _bulk: %v", operation))
			logger.Error().Msgf("Invalid JSON with operation definition in _bulk: %s", operation)
		}

	})

	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error processing _bulk: %v", err)
		return
	}

	for indexName, documents := range indicesWithDocumentsToInsert {
		phoneHomeAgent.IngestCounters().Add(indexName, int64(len(documents)))

		withConfiguration(ctx, cfg, indexName, make(types.JSON), func() error {
			for _, document := range documents {
				stats.GlobalStatistics.Process(cfg, indexName, document, clickhouse.NestedSeparator)
			}
			return lm.ProcessInsertQuery(ctx, indexName, documents)
		})
	}
	return results
}

var insertCounter = atomic.Int32{}

func withConfiguration(ctx context.Context, cfg config.QuesmaConfiguration, indexName string, body types.JSON, action func() error) {
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
