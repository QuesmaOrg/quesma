// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"fmt"
	chLib "github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"strings"
	"sync"
	"sync/atomic"
)

type SqlLowerer struct {
	virtualTableStorage       persistence.JSONDatabase
	ingestCounter             int64
	ingestFieldStatistics     IngestFieldStatistics
	ingestFieldStatisticsLock sync.Mutex
}

func NewSqlLowerer(virtualTableStorage persistence.JSONDatabase) *SqlLowerer {
	return &SqlLowerer{
		virtualTableStorage:   virtualTableStorage,
		ingestFieldStatistics: make(IngestFieldStatistics),
	}
}

func (ip *SqlLowerer) GenerateIngestContent(table *chLib.Table,
	data types.JSON,
	inValidJson types.JSON,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName) ([]AlterStatement, types.JSON, []NonSchemaField, error) {

	if len(table.Config.Attributes) == 0 {
		return nil, data, nil, nil
	}

	mDiff := DifferenceMap(data, table) // TODO change to DifferenceMap(m, t)

	if len(mDiff) == 0 && len(inValidJson) == 0 { // no need to modify, just insert 'js'
		return nil, data, nil, nil
	}

	// check attributes precondition
	if len(table.Config.Attributes) <= 0 {
		return nil, nil, nil, fmt.Errorf("no attributes config, but received non-schema fields: %s", mDiff)
	}
	attrsMap, _ := BuildAttrsMap(mDiff, table.Config)

	// generateNewColumns is called on original attributes map
	// before adding invalid fields to it
	// otherwise it would contain invalid fields e.g. with wrong types
	// we only want to add fields that are not part of the schema e.g we don't
	// have columns for them
	var alterStatements []AlterStatement
	atomic.AddInt64(&ip.ingestCounter, 1)
	if ok, alteredAttributesIndexes := ip.shouldAlterColumns(table, attrsMap); ok {
		alterStatements = ip.generateNewColumns(attrsMap, table, alteredAttributesIndexes, encodings)
	}
	// If there are some invalid fields, we need to add them to the attributes map
	// to not lose them and be able to store them later by
	// generating correct update query
	// addInvalidJsonFieldsToAttributes returns a new map with invalid fields added
	// this map is then used to generate non-schema fields string
	attrsMapWithInvalidFields := addInvalidJsonFieldsToAttributes(attrsMap, inValidJson)
	nonSchemaFields, err := generateNonSchemaFields(attrsMapWithInvalidFields)

	if err != nil {
		return nil, nil, nil, err
	}

	onlySchemaFields := RemoveNonSchemaFields(data, table)

	return alterStatements, onlySchemaFields, nonSchemaFields, nil
}

func (l *SqlLowerer) LowerToDDL(validatedJsons []types.JSON,
	table *chLib.Table,
	invalidJsons []types.JSON,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName,
	createTableCmd CreateTableStatement) ([]string, error) {
	var jsonsReadyForInsertion []string
	var alterStatements []AlterStatement

	for i, preprocessedJson := range validatedJsons {
		alter, onlySchemaFields, nonSchemaFields, err := l.GenerateIngestContent(table, preprocessedJson,
			invalidJsons[i], encodings)

		if err != nil {
			return nil, fmt.Errorf("error BuildInsertJson, tablename: '%s' : %v", table.Name, err)
		}
		insertJson, err := generateInsertJson(nonSchemaFields, onlySchemaFields)
		if err != nil {
			return nil, fmt.Errorf("error generatateInsertJson, tablename: '%s' json: '%s': %v", table.Name, PrettyJson(insertJson), err)
		}
		alterStatements = append(alterStatements, alter...)
		if err != nil {
			return nil, fmt.Errorf("error BuildInsertJson, tablename: '%s' json: '%s': %v", table.Name, PrettyJson(insertJson), err)
		}
		jsonsReadyForInsertion = append(jsonsReadyForInsertion, insertJson)
	}

	insertValues := strings.Join(jsonsReadyForInsertion, ", ")

	insertStatement := InsertStatement{
		TableName:    table.Name,
		InsertValues: insertValues,
	}

	return generateSqlStatements(createTableCmd, alterStatements, insertStatement), nil
}
