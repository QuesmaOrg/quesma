// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"math"
)

func removeLowCardinality(columnType string) string {
	if columnType == "LowCardinality(String)" {
		return "String"
	}
	return columnType
}

var integerTypes = map[string]bool{
	"UInt8":   true,
	"UInt16":  true,
	"UInt32":  true,
	"UInt64":  true,
	"UInt128": true,
	"UInt256": true,
	"Int8":    true,
	"Int16":   true,
	"Int32":   true,
	"Int64":   true,
	"Int128":  true,
	"Int256":  true,
}

var integerRange = map[string]struct {
	minAsInt64 int64 // Capped at int64 minimum value
	maxAsInt64 int64 // Capped at int64 maximum value
	minAsFloat float64
	maxAsFloat float64
}{
	"UInt8":   {0, math.MaxUint8, 0, math.MaxUint8},
	"UInt16":  {0, math.MaxUint16, 0, math.MaxUint16},
	"UInt32":  {0, math.MaxUint32, 0, math.MaxUint32},
	"UInt64":  {0, math.MaxInt64, 0, math.MaxUint64},
	"UInt128": {0, math.MaxInt64, 0, math.Pow(2, 128) - 1},
	"UInt256": {0, math.MaxInt64, 0, math.Pow(2, 256) - 1},
	"Int8":    {math.MinInt8, math.MaxInt8, math.MinInt8, math.MaxInt8},
	"Int16":   {math.MinInt16, math.MaxInt16, math.MinInt16, math.MaxInt16},
	"Int32":   {math.MinInt32, math.MaxInt32, math.MinInt32, math.MaxInt32},
	"Int64":   {math.MinInt64, math.MaxInt64, math.MinInt64, math.MaxInt64},
	"Int128":  {math.MinInt64, math.MaxInt64, -math.Pow(2, 128), math.Pow(2, 128) - 1},
	"Int256":  {math.MinInt64, math.MaxInt64, -math.Pow(2, 256), math.Pow(2, 256) - 1},
}

var floatingPointTypes = map[string]bool{
	"Float32": true,
	"Float64": true,
}

func isNumericType(columnType string) bool {
	return isIntegerType(columnType) || isFloatingPointType(columnType)
}

func isIntegerType(columnType string) bool {
	return integerTypes[columnType]
}

func isFloatingPointType(columnType string) bool {
	return floatingPointTypes[columnType]
}

func validateNumericRange(columnType string, value interface{}) (isValid bool) {
	columnRange, found := integerRange[columnType]
	if !found {
		panic(fmt.Sprintf("Unknown integer column type: %s", columnType))
	}

	switch v := value.(type) {
	case int64:
		return v >= columnRange.minAsInt64 && v <= columnRange.maxAsInt64
	case float64:
		return v >= columnRange.minAsFloat && v <= columnRange.maxAsFloat
	default:
		logger.Error().Msgf("Invalid value type for column of type %s: %T", columnType, value)
		return false
	}
}

func validateNumericType(columnType string, incomingValueType string, value interface{}) (isValid bool) {
	if isFloatingPointType(columnType) && isNumericType(incomingValueType) {
		return true
	}
	if isIntegerType(columnType) && isIntegerType(incomingValueType) {
		return validateNumericRange(columnType, value)
	}

	// numbers incoming as strings
	if isFloatingPointType(columnType) && incomingValueType == "String" {
		if valueAsStr, ok := value.(string); ok {
			return util.IsFloat(valueAsStr)
		} else {
			logger.Error().Msgf("Invalid value type for column of type %s: %T, value: %v", columnType, value, value)
		}
	}
	if isIntegerType(columnType) && incomingValueType == "String" {
		if valueAsStr, ok := value.(string); ok && util.IsInt(valueAsStr) {
			valueAsInt, err := util.ToInt64(valueAsStr)
			if err != nil {
				logger.Error().Msgf("Failed to convert value to int: %v", valueAsStr)
				return false
			}
			return validateNumericRange(columnType, valueAsInt)
		} else {
			logger.Error().Msgf("Invalid value type for column of type %s: %T, value: %v", columnType, value, value)
		}
	}

	return false
}

func validateValueAgainstType(fieldName string, value interface{}, columnType clickhouse.Type) (isValid bool) {
	switch columnType := columnType.(type) {
	case clickhouse.BaseType:
		incomingValueType, err := clickhouse.NewType(value, fieldName)
		if err != nil {
			return false
		}

		columnTypeName := removeLowCardinality(columnType.Name)

		if isNumericType(columnTypeName) {
			if incomingValueType, isBaseType := incomingValueType.(clickhouse.BaseType); isBaseType && validateNumericType(columnTypeName, incomingValueType.Name, value) {
				// Numeric types match!
				return true
			}
		}

		if incomingValueType, isBaseType := incomingValueType.(clickhouse.BaseType); isBaseType && incomingValueType.Name == columnTypeName {
			// Types match exactly!
			return true
		}

		return false
	case clickhouse.MultiValueType:
		if columnType.Name == "Tuple" {
			if value, isMap := value.(map[string]interface{}); isMap {
				for key, elem := range value {
					subtype := columnType.GetColumn(key)
					if subtype == nil {
						return false
					}
					if !validateValueAgainstType(fmt.Sprintf("%s.%s", fieldName, key), elem, subtype.Type) {
						return false
					}
				}
				return true
			}
		} else {
			logger.Error().Msgf("MultiValueType validation is not yet supported for type: %v", columnType)
		}

		return false
	case clickhouse.CompoundType:
		if columnType.Name == "Array" {
			if value, isArray := value.([]interface{}); isArray {
				for i, elem := range value {
					if !validateValueAgainstType(fmt.Sprintf("%s[%d]", fieldName, i), elem, columnType.BaseType) {
						return false
					}
				}
				return true
			}
		} else {
			logger.Error().Msgf("CompoundType validation is not yet supported for type: %v", columnType)
		}

		return false
	}

	return false
}

// validateIngest validates the document against the table schema
// and returns the fields that are not valid e.g. have wrong types
// according to the schema
func (ip *IngestProcessor) validateIngest(tableName string, document types.JSON) (types.JSON, error) {
	clickhouseTable := ip.FindTable(tableName)

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
			if !validateValueAgainstType(columnName, value, column.Type) {
				deletedFields[columnName] = value
			}
		}
	}
	return deletedFields, nil
}
