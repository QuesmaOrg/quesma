// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest_validator

import (
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
		return "Array(" + getTypeName(elem) + ")"
	}
	return goType
}

func removeLowCardinality(columnType string) string {
	if columnType == "LowCardinality(String)" {
		return "String"
	}
	return columnType
}

func (iv *IngestValidator) Transform(document types.JSON) (types.JSON, error) {
	clickhouseTable, ok := iv.tableMap.Load(iv.table)
	if !ok {
		logger.Error().Msgf("Table %s not found", iv.table)
		return document, nil
	}
	for k, v := range document {
		if v != nil {
			column := clickhouseTable.Cols[k]
			if column != nil {
				columnType := column.Type.String()
				columnType = removeLowCardinality(columnType)
				incomingValueType := getTypeName(v)
				if columnType == "DateTime64" {
					if incomingValueType != "String" {
						// validate date format
						logger.Error().Msgf("Field %s has wrong type %s, expected %s", k, getTypeName(v), columnType)
						return document, nil
					}
				} else if columnType != incomingValueType {
					logger.Error().Msgf("Field %s has wrong type %s, expected %s", k, getTypeName(v), columnType)
					return document, nil
				}
			}
		}
	}
	return document, nil
}

func (iv *IngestValidator) ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, tableMap clickhouse.TableMap, transformers []plugins.IngestTransformer) []plugins.IngestTransformer {
	transformers = append(transformers, &IngestValidator{cfg: cfg, schemaRegistry: schema, table: table, tableMap: tableMap})
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
