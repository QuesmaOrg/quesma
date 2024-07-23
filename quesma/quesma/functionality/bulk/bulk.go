// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
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
	"sync"
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

	bulkSize := len(bulk)
	maybeLogBatchSize(bulkSize / 2) // we divided payload by 2 so that we don't take into account the `action_and_meta_data` line, ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
	indicesWithDocumentsToInsert := make(map[string][]types.JSON, bulkSize)

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

		config.RunConfigured(ctx, cfg, indexName, make(types.JSON), func() error {
			for _, document := range documents {
				stats.GlobalStatistics.Process(cfg, indexName, document, clickhouse.NestedSeparator)
			}
			// if the index is mapped to specified database table in the configuration, use that table
			if len(cfg.IndexConfig[indexName].Override) > 0 {
				indexName = cfg.IndexConfig[indexName].Override
			}
			return lm.ProcessInsertQuery(ctx, indexName, documents)
		})
	}
	return results
}

// Global set to keep track of logged batch sizes
var loggedBatchSizes = make(map[int]struct{})
var mutex sync.Mutex

// maybeLogBatchSize logs only unique batch sizes
func maybeLogBatchSize(batchSize int) {
	mutex.Lock()
	defer mutex.Unlock()
	if _, alreadyLogged := loggedBatchSizes[batchSize]; !alreadyLogged {
		logger.Info().Msgf("Ingesting via _bulk API, batch size=%d documents", batchSize)
		loggedBatchSizes[batchSize] = struct{}{}
	}
}
