// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package doc

import (
	"context"
	"quesma/clickhouse"
	"quesma/ingest"
	"quesma/jsonprocessor"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/stats"
)

func Write(ctx context.Context, tableName string, body types.JSON, ip *ingest.IngestProcessor, cfg *config.QuesmaConfiguration) error {
	stats.GlobalStatistics.Process(cfg, tableName, body, clickhouse.NestedSeparator)

	defer recovery.LogPanic()
	if len(body) == 0 {
		return nil
	}

	return config.RunConfiguredIngest(ctx, cfg, tableName, body, func() error {
		if len(cfg.IndexConfig[tableName].Override) > 0 {
			tableName = cfg.IndexConfig[tableName].Override
		}
		nameFormatter := clickhouse.DefaultColumnNameFormatter()
		transformer := jsonprocessor.IngestTransformerFor(tableName, cfg)
		return ip.ProcessInsertQuery(ctx, tableName, types.NDJSON{body}, transformer, nameFormatter)
	})
}
