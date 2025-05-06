// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/end_user_errors"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/parsers/elastic_query_dsl"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/transformations"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/v2/core/diag"
	"github.com/QuesmaOrg/quesma/platform/v2/core/tracing"
	"github.com/goccy/go-json"
	"strconv"
	"time"
)

func handleTermsEnumInternal(ctx context.Context, index string, body types.JSON, lm clickhouse.LogManagerIFace,
	schemaRegistry schema.Registry, isFieldMapSyntaxEnabled bool, qmc diag.DebugInfoCollector) ([]byte, error) {
	if indices, err := lm.ResolveIndexPattern(ctx, schemaRegistry, index); err != nil || len(indices) != 1 { // multi index terms enum is not yet supported
		errorMsg := fmt.Sprintf("terms enum failed - could not resolve table name for index: %s", index)
		logger.ErrorFull(ctx, errorMsg, err)
		return nil, errors.New(errorMsg)
	} else {
		resolvedTableName := indices[0]
		resolvedSchema, ok := schemaRegistry.FindSchema(schema.IndexName(resolvedTableName))
		if !ok {
			return []byte{}, end_user_errors.ErrNoSuchSchema.New(fmt.Errorf("can't load %s schema", resolvedTableName)).Details("Table: %s", resolvedTableName)
		}

		return handleTermsEnumRequest(ctx, body, lm,
			&elastic_query_dsl.ClickhouseQueryTranslator{Table: lm.FindTable(indices[0]), Ctx: ctx, Schema: resolvedSchema},
			isFieldMapSyntaxEnabled, qmc)
	}
}

func handleTermsEnumRequest(ctx context.Context, body types.JSON, lm clickhouse.LogManagerIFace, qt *elastic_query_dsl.ClickhouseQueryTranslator,
	isFieldMapSyntaxEnabled bool, qmc diag.DebugInfoCollector) (result []byte, err error) {
	startTime := time.Now()

	// defaults as in:
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-terms-enum.html
	const (
		defaultSize            = 10
		defaultCaseInsensitive = false
	)

	var field string
	if fieldRaw, ok := body["field"]; ok {
		if field, ok = fieldRaw.(string); !ok {
			logger.ErrorWithCtx(ctx).Msgf("error reading terms enum API request body: field is not a string")
			return json.Marshal(emptyTermsEnumResponse())
		}
	} else {
		logger.ErrorWithCtx(ctx).Msgf("error reading terms enum API request body: field is not present")
		return json.Marshal(emptyTermsEnumResponse())
	}
	field = elastic_query_dsl.ResolveField(ctx, field, qt.Schema)

	size := defaultSize
	if sizeRaw, ok := body["size"]; ok {
		switch s := sizeRaw.(type) {
		case float64:
			size = int(s)
		case string:
			size, _ = strconv.Atoi(s)
		}
	}

	var prefixString *string
	if prefixStringRaw, ok := body["string"]; ok {
		if prefixStringParsed, ok2 := prefixStringRaw.(string); ok2 {
			prefixString = &prefixStringParsed
		}
	}

	caseInsensitive := defaultCaseInsensitive
	if caseInsensitiveRaw, ok := body["case_insensitive"]; ok {
		caseInsensitive, _ = caseInsensitiveRaw.(bool)
	}
	var indexFilter *map[string]interface{}
	if indexFilterRaw, ok := body["index_filter"]; ok {
		if indexFilterObj, ok2 := indexFilterRaw.(map[string]interface{}); ok2 {
			indexFilter = &indexFilterObj
		} else {
			logger.WarnWithCtx(ctx).Msgf("error reading terms enum API request body: index_filter is not a map")
		}
	}

	where := qt.ParseAutocomplete(indexFilter, field, prefixString, caseInsensitive)
	selectQuery := buildAutocompleteQuery(field, qt.Table.Name, where.WhereClause, size)
	selectQuery, err = transformations.ApplyAllNecessaryCommonTransformations(selectQuery, qt.Schema, isFieldMapSyntaxEnabled)
	if err == nil {
		dbQueryCtx, cancel := context.WithCancel(ctx)
		// TODO this will be used to cancel goroutine that is executing the query
		_ = cancel

		if rows, _, err2 := lm.ProcessQuery(dbQueryCtx, qt.Table, selectQuery); err2 != nil {
			logger.ErrorFull(ctx, "terms enum failed - error processing SQL query", err2)
			result, err = json.Marshal(emptyTermsEnumResponse())
		} else {
			result, err = json.Marshal(makeTermsEnumResponse(rows))
		}
	} else {
		logger.ErrorFull(ctx, "error applying necessary transformations", err)
		result, err = json.Marshal(emptyTermsEnumResponse())
	}

	path := ""
	if value := ctx.Value(tracing.RequestPath); value != nil {
		if str, ok := value.(string); ok {
			path = str
		}
	}

	ctxValues := tracing.ExtractValues(ctx)

	reqBody, _ := body.Bytes()
	qmc.PushSecondaryInfo(&diag.QueryDebugSecondarySource{
		Id:                     ctxValues.RequestId,
		Path:                   path,
		OpaqueId:               ctxValues.OpaqueId,
		IncomingQueryBody:      reqBody,
		QueryBodyTranslated:    []diag.TranslatedSQLQuery{{Query: []byte(selectQuery.SelectCommand.String())}},
		QueryTranslatedResults: result,
		SecondaryTook:          time.Since(startTime),
	})
	return
}

func makeTermsEnumResponse(rows []model.QueryResultRow) *model.TermsEnumResponse {
	terms := make([]string, 0)
	for _, row := range rows {
		value := row.Cols[0].Value
		if value != nil {
			switch val := value.(type) {
			case *string:
				terms = append(terms, *val)
			case string:
				terms = append(terms, val) // needed only for tests
			default:
				// may be e.g. Map(String, String). Elastic does nothing in that case, so let's do the same everywhere for now.
			}
		}
	}
	return &model.TermsEnumResponse{
		Complete: true,
		Terms:    terms,
	}
}

func emptyTermsEnumResponse() *model.TermsEnumResponse {
	return &model.TermsEnumResponse{
		Complete: false,
		Terms:    nil,
	}
}

func buildAutocompleteQuery(fieldName, tableName string, whereClause model.Expr, limit int) *model.Query {
	return &model.Query{
		SelectCommand: *model.NewSelectCommand(
			[]model.Expr{model.NewColumnRef(fieldName)},
			nil,
			nil,
			model.NewTableRef(tableName),
			whereClause,
			[]model.Expr{},
			limit,
			0,
			true,
			nil,
		),
	}
}
