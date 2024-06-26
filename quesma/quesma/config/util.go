// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"context"
	"quesma/logger"
	"quesma/quesma/types"
	"sync/atomic"
)

var insertCounter = atomic.Int32{}

func RunConfigured(ctx context.Context, cfg QuesmaConfiguration, indexName string, body types.JSON, action func() error) {
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

func findMatchingConfig(indexPattern string, cfg QuesmaConfiguration) (IndexConfiguration, bool) {
	matchCounter.Add(1)
	for _, indexConfig := range cfg.IndexConfig {
		if matchCounter.Load()%100 == 1 {
			logger.Debug().Msgf("matching index %s with config: %+v, ctr: %d", indexPattern, indexConfig.Name, matchCounter.Load())
		}
		if MatchName(indexPattern, indexConfig.Name) {
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
	return IndexConfiguration{}, false
}
