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
