// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"errors"
	"quesma/logger"
	"quesma/quesma/types"
	"reflect"
)

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
	case "int":
		if v.(int) < 0 {
			return "Int64"
		} else {
			return "UInt64"
		}
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
	case interface{}:
		if e := reflect.ValueOf(elem); e.Kind() == reflect.Slice {
			return "Array(" + getTypeName(e.Index(0).Interface()) + ")"
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

func validateValueAgainstType(fieldName string, value interface{}, column *Column) types.JSON {
	const DateTimeType = "DateTime64"
	const StringType = "String"
	deletedFields := make(types.JSON, 0)
	columnType := column.Type.String()
	columnType = removeLowCardinality(columnType)
	incomingValueType := getTypeName(value)
	if columnType == DateTimeType {
		// TODO validate date format
		// For now we store dates as strings
		if incomingValueType != StringType {
			deletedFields[fieldName] = value
		}
	} else if columnType != incomingValueType {
		deletedFields[fieldName] = value
	}
	return deletedFields
}

func (lm *LogManager) validateIngest(tableName string, document types.JSON) (types.JSON, error) {
	clickhouseTable := lm.FindTable(tableName)

	if clickhouseTable == nil {
		logger.Error().Msgf("Table %s not found", tableName)
		return nil, errors.New("table not found:" + tableName)
	}
	deletedFields := make(types.JSON)
	for columnName, column := range clickhouseTable.Cols {
		if column == nil {
			continue
		}
		if value, ok := document[columnName]; ok {
			if value == nil {
				continue
			}
			for k, v := range validateValueAgainstType(columnName, value, column) {
				deletedFields[k] = v
			}
		}
	}
	return deletedFields, nil
}
