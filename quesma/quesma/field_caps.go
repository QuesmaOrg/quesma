package quesma

import (
	"context"
	"encoding/json"
	"errors"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
)

const quesmaDebuggingFieldName = "QUESMA_CLICKHOUSE_RESPONSE"

func mapPrimitiveType(typeName string) string {
	switch typeName {
	case "DateTime", "DateTime64":
		return "date"
	case "String":
		return "text"
	case "Boolean":
		return "boolean"
	case "Int8":
		return "byte"
	case "Int16":
		return "short"
	case "Int32":
		return "integer"
	case "Int64":
		return "long"
	case "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256":
		return "unsigned_long"
	case "Float32":
		return "float"
	case "Float64":
		return "double"
	default:
		return typeName
	}
}

func getMostInnerType(compoundType clickhouse.Type) string {
	switch innerType := compoundType.(type) {
	case clickhouse.CompoundType:
		return getMostInnerType(innerType.BaseType)
	case clickhouse.MultiValueType:
		return "object"
	case clickhouse.BaseType:
		return mapPrimitiveType(innerType.String())
	}
	panic("unreachable")
}

func mapClickhouseToElasticType(col *clickhouse.Column) string {
	if col == nil {
		return "unknown"
	}
	colType := col.Type
	switch checkedType := colType.(type) {
	case clickhouse.BaseType:
		return mapPrimitiveType(checkedType.String())
	case clickhouse.CompoundType:
		return getMostInnerType(checkedType.BaseType)
	case clickhouse.MultiValueType:
		return "object"
	}

	return "unknown"
}

var aggregatableTypes = []string{
	"date", "byte", "short", "integer", "long", "unsigned_long", "float", "double",
}

func IsAggregatable(typeName string) bool {
	for _, t := range aggregatableTypes {
		if t == typeName {
			return true
		}
	}
	return false
}

func addNewFieldCapability(fields map[string]map[string]model.FieldCapability, col *clickhouse.Column) {
	if col == nil {
		return
	}
	typeName := mapClickhouseToElasticType(col)
	fieldCapability := model.FieldCapability{}
	fieldCapability.Aggregatable = IsAggregatable(typeName)
	// For now all fields are searchable
	fieldCapability.Searchable = true
	fieldCapability.MetadataField = new(bool)
	// We treat all fields as non-metadata ones
	*fieldCapability.MetadataField = false
	fieldCapability.Type = typeName
	fieldCapabilitiesMap := make(map[string]model.FieldCapability)
	fieldCapabilitiesMap[typeName] = fieldCapability
	fields[col.Name] = fieldCapabilitiesMap
}

func handleFieldCapsIndex(_ context.Context, resolvedIndex string, tables clickhouse.TableMap) ([]byte, error) {
	if len(resolvedIndex) == 0 {
		return nil, errors.New("unknown index : " + resolvedIndex)
	}

	fields := make(map[string]map[string]model.FieldCapability)
	if table, ok := tables[resolvedIndex]; ok {
		if table == nil {
			return nil, errors.New("could not find table for index : " + resolvedIndex)
		}
		for _, col := range table.Cols {
			addNewFieldCapability(fields, col)
		}
	}

	// Adding artificial quesma field
	quesmaCol := &clickhouse.Column{Name: quesmaDebuggingFieldName, Type: clickhouse.BaseType{Name: "String"}}
	addNewFieldCapability(fields, quesmaCol)

	fieldCapsResponse := model.FieldCapsResponse{Fields: fields}

	fieldCapsResponse.Indices = append(fieldCapsResponse.Indices, resolvedIndex)

	return json.MarshalIndent(fieldCapsResponse, "", "  ")
}

func hanndleFieldCaps(ctx context.Context, index string, _ []byte, lm *clickhouse.LogManager) ([]byte, error) {
	return handleFieldCapsIndex(ctx, lm.ResolveTableName(index), concatTableMap(lm.GetRuntimeTables(), lm.GetPredefinedTables()))
}

func concatTableMap(maps ...clickhouse.TableMap) clickhouse.TableMap {
	res := make(clickhouse.TableMap)
	for _, m := range maps {
		for k, v := range m {
			res[k] = v
		}
	}
	return res
}
