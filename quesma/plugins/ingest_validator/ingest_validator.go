// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest_validator

import (
	"errors"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/plugins"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"reflect"
)

type IngestValidator struct {
	cfg            config.QuesmaConfiguration
	schemaRegistry schema.Registry
	table          string
	tableMap       clickhouse.TableMap
}

func isInt(f float64) bool {
	return f == float64(int64(f))
}

func isUnsignedInt(f float64) bool {
	if f < 0 {
		return false
	}
	return f == float64(uint64(f))
}

func getTypeName(v interface{}) string {
	if v == nil {
		return "unknown"
	}
	goType := reflect.TypeOf(v).String()
	switch goType {
	case "string":
		return "String"
	case "bool":
		return "Bool"
	case "float64":
		if isInt(v.(float64)) {
			return "Int64"
		} else if isUnsignedInt(v.(float64)) {
			return "UInt64"
		} else {
			return "Float64"
		}
	}
	switch elem := v.(type) {
	case []interface{}:
		if len(elem) == 0 {
			return "Array(unknown)"
		} else {
			return "Array(" + getTypeName(elem[0]) + ")"
		}
	}

	return goType
}

func removeLowCardinality(columnType string) string {
	if columnType == "LowCardinality(String)" {
		return "String"
	}
	return columnType
}

func validateValueAgainstType(fieldName string, value interface{}, column *clickhouse.Column) []string {
	const DateTimeType = "DateTime64"
	const StringType = "String"
	deletedFields := make([]string, 0)
	columnType := column.Type.String()
	columnType = removeLowCardinality(columnType)
	incomingValueType := getTypeName(value)
	if columnType == DateTimeType {
		// TODO validate date format
		// For now we store dates as strings
		if incomingValueType != StringType {
			// We should store it as an attribute in the future
			deletedFields = append(deletedFields, fieldName)
		}
	} else if columnType != incomingValueType {
		// TODO remove field from document for now
		// We should store it as an attribute in the future
		deletedFields = append(deletedFields, fieldName)
	}
	return deletedFields
}

func (iv *IngestValidator) Transform(document types.JSON) (types.JSON, error) {

	clickhouseTable, ok := iv.tableMap.Load(iv.table)
	if !ok {
		logger.Error().Msgf("Table %s not found", iv.table)
		return nil, errors.New("table not found:" + iv.table)
	}
	deletedFields := make([]string, 0)
	for fieldName, value := range document {
		if value == nil {
			continue
		}
		column := clickhouseTable.Cols[fieldName]
		if column == nil {
			continue
		}
		deletedFields = append(deletedFields, validateValueAgainstType(fieldName, value, column)...)
	}
	for _, fieldName := range deletedFields {
		delete(document, fieldName)
	}
	return document, nil
}

func (iv *IngestValidator) ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, tableMap clickhouse.TableMap, transformers []plugins.IngestTransformer) []plugins.IngestTransformer {
	return transformers
}

func (iv *IngestValidator) ApplyQueryTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.QueryTransformer) []plugins.QueryTransformer {
	return transformers
}

func (iv *IngestValidator) ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.ResultTransformer) []plugins.ResultTransformer {
	return transformers
}

func (iv *IngestValidator) ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.FieldCapsTransformer) []plugins.FieldCapsTransformer {
	return transformers
}

func (iv *IngestValidator) GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration, schema schema.Registry) plugins.TableColumNameFormatter {
	return nil
}
