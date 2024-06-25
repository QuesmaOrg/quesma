package doc

import (
	"context"
	"quesma/clickhouse"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/stats"
)

func Write(ctx context.Context, tableName string, body types.JSON, lm *clickhouse.LogManager, cfg config.QuesmaConfiguration) error {
	stats.GlobalStatistics.Process(cfg, tableName, body, clickhouse.NestedSeparator)

	defer recovery.LogPanic()
	if len(body) == 0 {
		return nil
	}

	config.RunConfigured(ctx, cfg, tableName, body, func() error {
		return lm.ProcessInsertQuery(ctx, tableName, types.NDJSON{body})
	})
	return nil
}
