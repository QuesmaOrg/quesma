package doc

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/stats"
	"sync/atomic"
)

func Write(ctx context.Context, tableName string, body types.JSON, lm *clickhouse.LogManager, cfg config.QuesmaConfiguration) error {
	stats.GlobalStatistics.Process(cfg, tableName, body, clickhouse.NestedSeparator)

	defer recovery.LogPanic()
	if len(body) == 0 {
		return nil
	}

	withConfiguration(ctx, cfg, tableName, body, func() error {
		return lm.ProcessInsertQuery(ctx, tableName, types.NDJSON{body})
	})
	return nil
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
