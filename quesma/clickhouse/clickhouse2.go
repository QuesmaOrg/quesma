// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"fmt"
	"quesma/end_user_errors"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/schema"
	"quesma/telemetry"
	"quesma/util"
	quesma_api "quesma_v2/core"
	"slices"
	"strings"
	"time"
)

type (
	LogManager2 struct {
		ctx            context.Context
		cancel         context.CancelFunc
		chDb           quesma_api.BackendConnector
		tableDiscovery TableDiscovery
		cfg            *config.QuesmaConfiguration
		phoneHomeAgent telemetry.PhoneHomeAgent
	}
)

type LogManagerIFace interface {
	ResolveIndexPattern(ctx context.Context, schema schema.Registry, pattern string) (results []string, err error)
}

func (lm *LogManager2) Ping() error {
	return lm.chDb.Open()
}

func (lm *LogManager2) Start() {
	if err := lm.Ping(); err != nil {
		endUserError := end_user_errors.GuessClickhouseErrorType(err)
		logger.ErrorWithCtxAndReason(lm.ctx, endUserError.Reason()).Msgf("could not connect to clickhouse. error: %v", endUserError)
	}

	lm.tableDiscovery.ReloadTableDefinitions()

	logger.Info().Msgf("schemas loaded: %s", lm.tableDiscovery.TableDefinitions().Keys())
	const reloadInterval = 1 * time.Minute
	forceReloadCh := lm.tableDiscovery.ForceReloadCh()

	go func() {
		recovery.LogPanic()
		for {
			select {
			case <-lm.ctx.Done():
				logger.Debug().Msg("closing log manager")
				return
			case doneCh := <-forceReloadCh:
				// this prevents flood of reloads, after a long pause
				if time.Since(lm.tableDiscovery.LastReloadTime()) > reloadInterval {
					lm.tableDiscovery.ReloadTableDefinitions()
				}
				doneCh <- struct{}{}
			case <-time.After(reloadInterval):
				// only reload if we actually use Quesma, make it double time to prevent edge case
				// otherwise it prevent ClickHouse Cloud from idle pausing
				if time.Since(lm.tableDiscovery.LastAccessTime()) < reloadInterval*2 {
					lm.tableDiscovery.ReloadTableDefinitions()
				}
			}
		}
	}()
}

func (lm *LogManager2) Stop() {
	lm.cancel()
}

func (lm *LogManager2) ReloadTables() {
	logger.Info().Msg("reloading tables definitions")
	lm.tableDiscovery.ReloadTableDefinitions()
}

func (lm *LogManager2) Close() {
	_ = lm.chDb.Close()
}

// ResolveIndexPattern - takes incoming index pattern (e.g. "index-*" or multiple patterns like "index-*,logs-*")
// and returns all matching indexes. Empty pattern means all indexes, "_all" index name means all indexes
//
//	Note: Empty pattern means all indexes, "_all" index name means all indexes
func (lm *LogManager2) ResolveIndexPattern(ctx context.Context, schema schema.Registry, pattern string) (results []string, err error) {
	if err = lm.tableDiscovery.TableDefinitionsFetchError(); err != nil {
		return nil, err
	}

	results = make([]string, 0)
	if strings.Contains(pattern, ",") {
		for _, pattern := range strings.Split(pattern, ",") {
			if pattern == allElasticsearchIndicesPattern || pattern == "" {
				for k := range schema.AllSchemas() {
					results = append(results, k.AsString())
				}
				slices.Sort(results)
				return results, nil
			} else {
				indexes, err := lm.ResolveIndexPattern(ctx, schema, pattern)
				if err != nil {
					return nil, err
				}
				results = append(results, indexes...)
			}
		}
	} else {
		if pattern == allElasticsearchIndicesPattern || len(pattern) == 0 {
			for k := range schema.AllSchemas() {
				results = append(results, k.AsString())
			}
			slices.Sort(results)
			return results, nil
		} else {
			for schemaName := range schema.AllSchemas() {
				matches, err := util.IndexPatternMatches(pattern, schemaName.AsString())
				if err != nil {
					logger.Error().Msgf("error matching index pattern: %v", err)
				}
				if matches {
					results = append(results, schemaName.AsString())
				}
			}
		}
	}

	return util.Distinct(results), nil
}

func (lm *LogManager2) CountMultiple(ctx context.Context, tables ...string) (int64, error) {
	if len(tables) == 0 {
		return 0, nil
	}
	const subcountStatement = "(SELECT count(*) FROM ?)"
	var subCountStatements []string
	for range len(tables) {
		subCountStatements = append(subCountStatements, subcountStatement)
	}

	var count int64
	var anyTables []any
	for _, t := range tables {
		anyTables = append(anyTables, t)
	}

	res, err := lm.chDb.Query(ctx, fmt.Sprintf("SELECT sum(*) as count FROM (%s)", strings.Join(subCountStatements, " UNION ALL ")), anyTables...)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	res.Scan(&count)
	return count, nil
}

func (lm *LogManager2) Count(ctx context.Context, table string) (int64, error) {
	var count int64
	res, err := lm.chDb.Query(ctx, "SELECT count(*) FROM ?", table)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	res.Scan(&count)
	return count, nil
}

//func (lm *LogManager2) executeRawQuery(query string) (*sql.Rows, error) {
//	if res, err := lm.chDb.Query(context.Background(), query); err != nil {
//		return nil, fmt.Errorf("error in executeRawQuery: query: %s\nerr:%v", query, err)
//	} else {
//		return res, nil
//	}
//}

func (lm *LogManager2) GetDB() quesma_api.BackendConnector {
	return lm.chDb
}

func (lm *LogManager2) FindTable(tableName string) (result *Table) {
	tableNamePattern := util.TableNamePatternRegexp(tableName)
	lm.tableDiscovery.TableDefinitions().
		Range(func(name string, table *Table) bool {
			if tableNamePattern.MatchString(name) {
				result = table
				return false
			}
			return true
		})

	return result
}

func (lm *LogManager2) GetTableDefinitions() (TableMap, error) {
	if err := lm.tableDiscovery.TableDefinitionsFetchError(); err != nil {
		return *lm.tableDiscovery.TableDefinitions(), err
	}

	return *lm.tableDiscovery.TableDefinitions(), nil
}

// Returns if schema wasn't created (so it needs to be, and will be in a moment)
func (lm *LogManager2) AddTableIfDoesntExist(table *Table) bool {
	t := lm.FindTable(table.Name)
	if t == nil {
		table.Created = true

		table.ApplyIndexConfig(lm.cfg)

		lm.tableDiscovery.TableDefinitions().Store(table.Name, table)
		return true
	}
	wasntCreated := !t.Created
	t.Created = true
	return wasntCreated
}

func NewEmptyLogManager2(cfg *config.QuesmaConfiguration, chDb quesma_api.BackendConnector, phoneHomeAgent telemetry.PhoneHomeAgent, loader TableDiscovery) *LogManager2 {
	ctx, cancel := context.WithCancel(context.Background())
	return &LogManager2{ctx: ctx, cancel: cancel, chDb: chDb, tableDiscovery: loader, cfg: cfg, phoneHomeAgent: phoneHomeAgent}
}

//func NewLogManager2(tables *TableMap, cfg *config.QuesmaConfiguration) *LogManager2 {
//	var tableDefinitions = atomic.Pointer[TableMap]{}
//	tableDefinitions.Store(tables)
//	return &LogManager2{chDb: nil, tableDiscovery: NewTableDiscoveryWith(cfg, nil, *tables),
//		cfg: cfg, phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(),
//	}
//}

func (l *LogManager2) IsInTransparentProxyMode() bool {
	return l.cfg.TransparentProxy
}

func (lm *LogManager2) explainQuery(ctx context.Context, query string, elapsed time.Duration) string {

	explainQuery := "EXPLAIN json=1, indexes=1 " + query

	rows, err := lm.chDb.Query(ctx, explainQuery)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("failed to explain slow query: %v", err)
	}
	var explain string

	defer rows.Close()
	if rows.Next() {
		err := rows.Scan(&explain)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("failed to scan slow query explain: %v", err)
			return ""
		}

		// reformat the explain output to make it one line and more readable
		explain = strings.ReplaceAll(explain, "\n", "")
		explain = strings.ReplaceAll(explain, "  ", "")

		logger.WarnWithCtx(ctx).Msgf("slow query (time: '%s')  query: '%s' -> explain: '%s'", elapsed, query, explain)
	}

	if rows.Err() != nil {
		logger.ErrorWithCtx(ctx).Msgf("failed to read slow query explain: %v", rows.Err())
	}
	return explain
}
