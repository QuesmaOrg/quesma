// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/end_user_errors"
	quesma_errors "github.com/QuesmaOrg/quesma/platform/errors"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/parsers/elastic_query_dsl"
	"github.com/QuesmaOrg/quesma/platform/schema"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
)

func (q *QueryRunner) clickhouseConnectorFromDecision(ctx context.Context, decision *quesma_api.Decision) (
	clickhouseConnector *quesma_api.ConnectorDecisionClickhouse, err error) {

	for _, connector := range decision.UseConnectors {
		switch c := connector.(type) {

		case *quesma_api.ConnectorDecisionClickhouse:
			clickhouseConnector = c

		case *quesma_api.ConnectorDecisionElastic:
		// After https://github.com/QuesmaOrg/quesma/pull/1278 we should never land in this situation,
		// previously this was an escape hatch for `_msearch` payload containing Elasticsearch-targetted query
		// This code lives only to postpone bigger refactor of `handleSearchCommon` which also supports async and A/B testing

		default:
			return nil, fmt.Errorf("unknown connector type: %T", c)
		}
	}

	if clickhouseConnector == nil {
		logger.WarnWithCtx(ctx).Msgf("multi-search payload contains Elasticsearch-targetted query")
		return nil, fmt.Errorf("quesma-processed _msearch payload contains Elasticsearch-targetted query")
	}

	return clickhouseConnector, nil
}

func (q *QueryRunner) checkDecision(ctx context.Context, decision *quesma_api.Decision, optAsync *AsyncQuery) (
	respIfWeEndSearch []byte, err error, weEndSearch bool) {

	if decision.Err != nil {
		if optAsync != nil {
			respIfWeEndSearch, _ = elastic_query_dsl.EmptyAsyncSearchResponse(optAsync.asyncId, false, 200)
		} else {
			respIfWeEndSearch = elastic_query_dsl.EmptySearchResponse(ctx)
		}
		return respIfWeEndSearch, decision.Err, true
	}

	if decision.IsEmpty {
		if optAsync != nil {
			respIfWeEndSearch, err = elastic_query_dsl.EmptyAsyncSearchResponse(optAsync.asyncId, false, 200)
		} else {
			respIfWeEndSearch = elastic_query_dsl.EmptySearchResponse(ctx)
		}
		return respIfWeEndSearch, err, true
	}

	if decision.IsClosed {
		err = quesma_errors.ErrIndexNotExists() // TODO
		return []byte{}, err, true
	}

	if len(decision.UseConnectors) == 0 {
		err = end_user_errors.ErrSearchCondition.New(fmt.Errorf("no connectors to use"))
		return []byte{}, err, true
	}

	return []byte{}, nil, false
}

func (q *QueryRunner) resolveIndexes(ctx context.Context, clickhouseConnector *quesma_api.ConnectorDecisionClickhouse, tables database_common.TableMap,
	optAsync *AsyncQuery) (resolvedIndexes []string, currentSchema schema.Schema, table *database_common.Table, respWhenError []byte, err error) {

	if clickhouseConnector.IsCommonTable {
		return q.resolveIndexesCommonTable(ctx, clickhouseConnector, tables, optAsync)
	} else {
		return q.resolveIndexesNonCommonTable(ctx, clickhouseConnector, tables)
	}
}

func (q *QueryRunner) resolveIndexesNonCommonTable(ctx context.Context, clickhouseConnector *quesma_api.ConnectorDecisionClickhouse,
	tables database_common.TableMap) (resolvedIndexes []string, currentSchema schema.Schema, table *database_common.Table, respWhenError []byte, err error) {

	resolvedIndexes = clickhouseConnector.ClickhouseIndexes
	if len(resolvedIndexes) < 1 {
		err = end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load [%s] schema", resolvedIndexes)).Details("Table: [%v]", resolvedIndexes)
		return
	}

	indexName := resolvedIndexes[0] // we got exactly one table here because of the check above (much later: for sure?)
	if len(resolvedIndexes) > 1 {
		logger.WarnWithCtx(ctx).Msgf("multiple indexes in search request, using the first one: %s", indexName)
	}

	resolvedTableName := q.cfg.IndexConfig[indexName].TableName(indexName)
	resolvedSchema, ok := q.schemaRegistry.FindSchema(schema.IndexName(indexName))
	if !ok {
		err = end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s schema", resolvedTableName)).Details("Table: %s", resolvedTableName)
		return
	}

	if table, _ = tables.Load(resolvedTableName); table == nil {
		err = end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s table", resolvedTableName)).Details("Table: %s", resolvedTableName)
		return
	}

	// Clone the resolved schema to currentSchema
	//
	// Schema can be modified during the query execution, we don't want to modify the original schema, and we don't need any concurrency issues here.
	//
	// resolveIndexesCommonTable also returns an ephemeral schema

	currentSchema = schema.Schema{
		Fields:             make(map[schema.FieldName]schema.Field),
		Aliases:            make(map[schema.FieldName]schema.FieldName),
		ExistsInDataSource: resolvedSchema.ExistsInDataSource,
		DatabaseName:       resolvedSchema.DatabaseName,
	}

	for fieldName, field := range resolvedSchema.Fields {
		currentSchema.Fields[fieldName] = field
	}

	for aliasName, targetFieldName := range resolvedSchema.Aliases {
		currentSchema.Aliases[aliasName] = targetFieldName
	}

	return
}

func (q *QueryRunner) resolveIndexesCommonTable(ctx context.Context, clickhouseConnector *quesma_api.ConnectorDecisionClickhouse, tables database_common.TableMap,
	optAsync *AsyncQuery) (resolvedIndexes []string, currentSchema schema.Schema, table *database_common.Table, respWhenError []byte, err error) {

	// here we filter out indexes that are not stored in the common table
	var virtualOnlyTables []string
	resolvedIndexes = clickhouseConnector.ClickhouseIndexes
	for _, indexName := range resolvedIndexes {
		tabl, _ := tables.Load(q.cfg.IndexConfig[indexName].TableName(indexName))
		if tabl == nil {
			continue
		}
		if tabl.VirtualTable {
			virtualOnlyTables = append(virtualOnlyTables, indexName)
		}
	}

	resolvedIndexes = virtualOnlyTables
	if len(resolvedIndexes) == 0 {
		if optAsync != nil {
			respWhenError, err = elastic_query_dsl.EmptyAsyncSearchResponse(optAsync.asyncId, false, 200)
		} else {
			respWhenError, err = elastic_query_dsl.EmptySearchResponse(ctx), nil
		}
		return
	}

	commonTable, ok := tables.Load(common_table.TableName)
	if !ok {
		err = end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s table", common_table.TableName)).Details("Table: %s", common_table.TableName)
		return
	}

	// Let's build a  union of schemas
	resolvedSchema := schema.Schema{
		Fields:             make(map[schema.FieldName]schema.Field),
		Aliases:            make(map[schema.FieldName]schema.FieldName),
		ExistsInDataSource: false,
		DatabaseName:       "", // it doesn't matter here, common table will be used
	}

	schemas := q.schemaRegistry.AllSchemas()

	for _, idx := range resolvedIndexes {
		if scm, exists := schemas[schema.IndexName(idx)]; exists {
			for fieldName := range scm.Fields {
				// here we construct our runtime  schema by merging fields from all resolved indexes
				resolvedSchema.Fields[fieldName] = scm.Fields[fieldName]
			}
		} else {
			err = end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s schema", idx)).Details("Table: %s", idx)
			return
		}
	}

	currentSchema = resolvedSchema
	table = commonTable

	return
}
