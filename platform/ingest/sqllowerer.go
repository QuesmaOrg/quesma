// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/comment_metadata"
	chLib "github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/logger"
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

// This function generates ALTER TABLE commands for adding new columns
// to the table based on the attributesMap and the table name
// AttributesMap contains the attributes that are not part of the schema
// Function has side effects, it modifies the table.Cols map
// and removes the attributes that were promoted to columns
func (ip *SqlLowerer) generateNewColumns(
	attrsMap map[string][]interface{},
	table *chLib.Table,
	alteredAttributesIndexes []int,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName) []AlterStatement {
	var alterStatements []AlterStatement
	attrKeys := getAttributesByArrayName(chLib.DeprecatedAttributesKeyColumn, attrsMap)
	attrTypes := getAttributesByArrayName(chLib.DeprecatedAttributesValueType, attrsMap)
	var deleteIndexes []int

	reverseMap := reverseFieldEncoding(encodings, table.Name)

	// HACK Alert:
	// We must avoid altering the table.Cols map and reading at the same time.
	// This should be protected by a lock or a copy of the table should be used.
	//
	newColumns := make(map[string]*chLib.Column)
	for k, v := range table.Cols {
		newColumns[k] = v
	}

	for i := range alteredAttributesIndexes {

		columnType := ""
		modifiers := ""

		if attrTypes[i] == chLib.UndefinedType {
			continue
		}

		// Array and Map are not Nullable
		if strings.Contains(attrTypes[i], "Array") || strings.Contains(attrTypes[i], "Map") {
			columnType = attrTypes[i]
		} else {
			modifiers = "Nullable"
			columnType = fmt.Sprintf("Nullable(%s)", attrTypes[i])
		}

		propertyName := attrKeys[i]
		field, ok := reverseMap[schema.EncodedFieldName(attrKeys[i])]
		if ok {
			propertyName = field.FieldName
		}

		metadata := comment_metadata.NewCommentMetadata()
		metadata.Values[comment_metadata.ElasticFieldName] = propertyName
		comment := metadata.Marshall()

		alterColumn := AlterStatement{
			Type:       AddColumn,
			TableName:  table.Name,
			OnCluster:  table.ClusterName,
			ColumnName: attrKeys[i],
			ColumnType: columnType,
		}
		newColumns[attrKeys[i]] = &chLib.Column{Name: attrKeys[i], Type: chLib.NewBaseType(attrTypes[i]), Modifiers: modifiers, Comment: comment}
		alterStatements = append(alterStatements, alterColumn)

		alterColumnComment := AlterStatement{
			Type:       CommentColumn,
			TableName:  table.Name,
			OnCluster:  table.ClusterName,
			ColumnName: attrKeys[i],
			Comment:    comment,
		}
		alterStatements = append(alterStatements, alterColumnComment)

		deleteIndexes = append(deleteIndexes, i)
	}

	table.Cols = newColumns

	if table.VirtualTable {
		err := storeVirtualTable(table, ip.virtualTableStorage)
		if err != nil {
			logger.Error().Msgf("error storing virtual table: %v", err)
		}
	}

	for i := len(deleteIndexes) - 1; i >= 0; i-- {
		attrsMap[chLib.DeprecatedAttributesKeyColumn] = append(attrsMap[chLib.DeprecatedAttributesKeyColumn][:deleteIndexes[i]], attrsMap[chLib.DeprecatedAttributesKeyColumn][deleteIndexes[i]+1:]...)
		attrsMap[chLib.DeprecatedAttributesValueType] = append(attrsMap[chLib.DeprecatedAttributesValueType][:deleteIndexes[i]], attrsMap[chLib.DeprecatedAttributesValueType][deleteIndexes[i]+1:]...)
		attrsMap[chLib.DeprecatedAttributesValueColumn] = append(attrsMap[chLib.DeprecatedAttributesValueColumn][:deleteIndexes[i]], attrsMap[chLib.DeprecatedAttributesValueColumn][deleteIndexes[i]+1:]...)
	}
	return alterStatements
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
