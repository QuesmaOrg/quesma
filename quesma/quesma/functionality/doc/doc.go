// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package doc

import (
	"context"
	"quesma/clickhouse"
	"quesma/jsonprocessor"
	"quesma/logger"
	"quesma/plugins/registry"
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
		if len(cfg.IndexConfig[tableName].Override) > 0 {
			tableName = cfg.IndexConfig[tableName].Override
		}
		nameFormatter, err := registry.TableColumNameFormatterFor(tableName, cfg, nil)
		if err != nil {
			logger.Error().Msgf("Error getting table column name formatter for index %s: %v", tableName, err)
			return err
		}

		transformer := jsonprocessor.IngestTransformerFor(tableName, cfg)
		return lm.ProcessInsertQuery(ctx, tableName, types.NDJSON{body}, transformer, nameFormatter)
	})
	return nil
}
