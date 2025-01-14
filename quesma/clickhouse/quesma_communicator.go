// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/k0kubun/pp"
	"math/rand"
	"quesma/end_user_errors"
	"quesma/logger"
	"quesma/model"
	"quesma/quesma/recovery"
	quesma_api "quesma_v2/core"
	tracing "quesma_v2/core/tracing"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Implementation of API for Quesma

type FieldInfo = int

const (
	NotExists FieldInfo = iota
	ExistsAndIsBaseType
	ExistsAndIsArray
)

type PerformanceResult struct {
	QueryID      string
	Duration     time.Duration
	RowsReturned int
	ExplainPlan  string
	Error        error
}

// ProcessQuery - only WHERE clause
// TODO query param should be type safe Query representing all parts of
// sql statement that were already parsed and not string from which
// we have to extract again different parts like where clause and columns to build a proper result
func (lm *LogManager) ProcessQuery(ctx context.Context, table *Table, query *model.Query) (rows []model.QueryResultRow, performanceResult PerformanceResult, err error) {
	pp.Println(query)
	rowToScan := make([]interface{}, len(query.SelectCommand.Columns))
	columns := make([]string, 0, len(query.SelectCommand.Columns))

	for count, col := range query.SelectCommand.Columns {
		var colName string

		switch col := col.(type) {
		case model.ColumnRef:
			colName = col.ColumnName
		case model.AliasedExpr:
			colName = col.Alias
		case model.LiteralExpr:

			// There's now a AliasColumnsTransformation transformation that handles this,
			// but it's not fully complete as it'll require to change a lot more tests.
			//
			// The only remaining issue is that Pancake SQLs sometimes generate LiteralExpr in SELECT
			// instead of ColumnRef (nested SQLs case).

			if str, isStr := col.Value.(string); isStr {
				if unquoted, err := strconv.Unquote(str); err == nil {
					colName = unquoted
				} else {
					colName = str
				}
			} else {
				// AliasColumnsTransformation should have handled this
				logger.Warn().Msgf("Unexpected unaliased literal: %v", col.Value)
				if colName == "" {
					colName = fmt.Sprintf("column_%d", count)
				}
			}
		default:
			// AliasColumnsTransformation should have handled this
			logger.Warn().Msgf("Unexpected unaliased literal: %v", col)
			if colName == "" {
				colName = fmt.Sprintf("column_%d", count)
			}
		}

		columns = append(columns, colName)

	}

	rows, performanceResult, err = executeQuery(ctx, lm, query, columns, rowToScan)

	if err == nil {
		for _, row := range rows {
			row.Index = table.Name
		}
	}
	return rows, performanceResult, err
}

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

const slowQueryThreshold = 30 * time.Second
const slowQuerySampleRate = 0.1

func shouldExplainQuery(elapsed time.Duration) bool {
	return elapsed > slowQueryThreshold && random.Float64() < slowQuerySampleRate
}

func (lm *LogManager) explainQuery(ctx context.Context, query string, elapsed time.Duration) string {

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

var queryCounter atomic.Int64

func getQueryId(ctx context.Context) string {
	prefix := "quesma-"

	if asyncId, ok := ctx.Value(tracing.AsyncIdCtxKey).(string); ok {
		prefix = asyncId
	} else if requestId, ok := ctx.Value(tracing.RequestIdCtxKey).(string); ok {
		prefix = requestId
	}

	return fmt.Sprintf("%s-%d", prefix, queryCounter.Add(1))
}

func executeQuery(ctx context.Context, lm *LogManager, query *model.Query, fields []string, rowToScan []interface{}) (res []model.QueryResultRow, performanceResult PerformanceResult, err error) {
	span := lm.phoneHomeAgent.ClickHouseQueryDuration().Begin()

	queryAsString := query.SelectCommand.String()

	// We drop privileges for the query
	//
	// https://clickhouse.com/docs/en/operations/settings/permissions-for-queries
	//

	settings := make(clickhouse.Settings)
	// this "readonly" setting turned out to be causing problems with Hydrolix queries
	// the queries looked pretty legit, but the key difference was the use of schema (`FROM "schema"."tableName"`)
	// to be revisited in the future
	// settings["readonly"] = "1"
	settings["allow_ddl"] = "0"

	if query.OptimizeHints != nil {
		for k, v := range query.OptimizeHints.ClickhouseQuerySettings {
			settings[k] = v
		}

		if len(query.OptimizeHints.OptimizationsPerformed) > 0 {
			queryAsString = queryAsString + "\n-- optimizations: " + strings.Join(query.OptimizeHints.OptimizationsPerformed, ", ") + "\n"
		}
	}

	queryID := getQueryId(ctx)
	performanceResult.QueryID = queryID

	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(settings), clickhouse.WithQueryID(queryID))

	rows, err := lm.chDb.Query(ctx, queryAsString)
	if err != nil {
		elapsed := span.End(err)
		performanceResult.Duration = elapsed
		performanceResult.Error = err
		return nil, performanceResult, end_user_errors.GuessClickhouseErrorType(err).InternalDetails("clickhouse: query failed. err: %v, query: %v", err, queryAsString)
	}
	res, err = read(ctx, rows, fields, rowToScan, query.SelectCommand.Limit)

	elapsed := span.End(nil)
	performanceResult.Duration = elapsed
	performanceResult.RowsReturned = len(res)
	if err == nil {
		if shouldExplainQuery(elapsed) {
			performanceResult.ExplainPlan = lm.explainQuery(ctx, queryAsString, elapsed)
		}
	}

	return res, performanceResult, err
}

// 'selectFields' are all values that we return from the query, both columns and non-schema fields,
// like e.g. count(), or toInt8(boolField)
func read(ctx context.Context, rows quesma_api.Rows, selectFields []string, rowToScan []interface{}, limit int) ([]model.QueryResultRow, error) {

	// read selected fields from the metadata

	rowDb := make([]interface{}, 0, len(rowToScan))
	for i := range rowToScan {
		rowDb = append(rowDb, &rowToScan[i])
	}
	resultRows := make([]model.QueryResultRow, 0)
	// If a limit is set (limit != 0) then collect only the first 'limit' rows
	for (len(resultRows) < limit || limit == 0) && rows.Next() {
		err := rows.Scan(rowDb...)
		if err != nil {
			return nil, fmt.Errorf("clickhouse: scan failed: %v", err)
		}
		resultRow := model.QueryResultRow{Cols: make([]model.QueryResultCol, len(selectFields))}
		for i, field := range selectFields {
			resultRow.Cols[i] = model.QueryResultCol{ColName: field, Value: rowToScan[i]}
		}
		resultRows = append(resultRows, resultRow)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("clickhouse: iterating over rows failed:  %v", rows.Err())
	}
	go func() {
		defer recovery.LogPanicWithCtx(ctx)
		err := rows.Close()
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("clickhouse: closing rows failed: %v", err)
		}
	}()
	return resultRows, nil
}
