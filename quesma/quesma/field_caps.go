package quesma

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/util"
	"slices"
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

func addNewDefaultFieldCapability(fields map[string]map[string]model.FieldCapability, col *clickhouse.Column, index string) {
	typeName := mapClickhouseToElasticType(col)
	fieldCapability := model.FieldCapability{Indices: []string{index}}
	fieldCapability.Aggregatable = IsAggregatable(typeName)
	// For now all fields are searchable
	fieldCapability.Searchable = true
	// We treat all fields as non-metadata ones
	fieldCapability.MetadataField = util.Pointer(false)
	fieldCapability.Type = typeName

	if _, exists := fields[col.Name]; !exists {
		fields[col.Name] = make(map[string]model.FieldCapability)
	}

	if existing, exists := fields[col.Name][typeName]; exists {
		merged, ok := merge(existing, fieldCapability)
		if ok {
			fields[col.Name][typeName] = merged
		}
	} else {
		fields[col.Name][typeName] = fieldCapability
	}
}

func canBeKeywordField(col *clickhouse.Column) bool {
	typeName := mapClickhouseToElasticType(col)
	return typeName == "text" || typeName == "LowCardinality(String)"
}

func addNewKeywordFieldCapability(fields map[string]map[string]model.FieldCapability, col *clickhouse.Column, index string) {
	var keyword = model.FieldCapability{
		Aggregatable: true,
		Searchable:   true,
		Type:         "keyword",
		Indices:      []string{index},
	}
	if _, exists := fields[col.Name]; !exists {
		fields[col.Name] = make(map[string]model.FieldCapability)
	}
	if existing, exists := fields[col.Name]["keyword"]; exists {
		merged, ok := merge(existing, keyword)
		if ok {
			fields[col.Name]["keyword"] = merged
		}
	} else {
		fields[col.Name]["keyword"] = keyword
	}
}

func handleFieldCapsIndex(ctx context.Context, indexes []string, tables clickhouse.TableMap) ([]byte, error) {
	fields := make(map[string]map[string]model.FieldCapability)
	for _, resolvedIndex := range indexes {
		if len(resolvedIndex) == 0 {
			continue
		}

		if table, ok := tables.Load(resolvedIndex); ok {
			if table == nil {
				return nil, errors.New("could not find table for index : " + resolvedIndex)
			}

			for _, col := range table.Cols {

				if col == nil {
					continue
				}

				if isInternalColumn(col) {
					continue
				}

				if canBeKeywordField(col) {
					addNewKeywordFieldCapability(fields, col, resolvedIndex)
				} else {
					addNewDefaultFieldCapability(fields, col, resolvedIndex)
				}
			}

			for _, alias := range table.AliasFields(ctx) {
				if alias == nil {
					continue
				}

				if canBeKeywordField(alias) {
					addNewKeywordFieldCapability(fields, alias, resolvedIndex)
				} else {
					addNewDefaultFieldCapability(fields, alias, resolvedIndex)
				}
			}
		}

		quesmaCol := &clickhouse.Column{Name: quesmaDebuggingFieldName, Type: clickhouse.BaseType{Name: "String"}}
		addNewDefaultFieldCapability(fields, quesmaCol, resolvedIndex)
	}

	fieldCapsResponse := model.FieldCapsResponse{Fields: fields}
	fieldCapsResponse.Indices = append(fieldCapsResponse.Indices, indexes...)
	return json.Marshal(fieldCapsResponse)
}

func EmptyFieldCapsResponse() []byte {
	var response = model.FieldCapsResponse{
		Fields:  make(map[string]map[string]model.FieldCapability),
		Indices: []string{},
	}
	if serialized, err := json.Marshal(response); err != nil {
		panic(fmt.Sprintf("Failed to serialize empty field caps response: %v, this should never happen", err))
	} else {
		return serialized
	}
}

func isInternalColumn(col *clickhouse.Column) bool {
	return col.Name == clickhouse.AttributesKeyColumn || col.Name == clickhouse.AttributesValueColumn
}

func handleFieldCaps(ctx context.Context, index string, lm *clickhouse.LogManager) ([]byte, error) {
	indexes := lm.ResolveIndexes(ctx, index)
	if len(indexes) == 0 {
		if !elasticsearch.IsIndexPattern(index) {
			return nil, errIndexNotExists
		}
	}
	return handleFieldCapsIndex(ctx, indexes, lm.GetTableDefinitions())
}

func merge(cap1, cap2 model.FieldCapability) (model.FieldCapability, bool) {
	if cap1.Type != cap2.Type {
		return model.FieldCapability{}, false
	}
	var indices []string
	indices = append(indices, cap1.Indices...)
	indices = append(indices, cap2.Indices...)
	slices.Sort(indices)
	indices = slices.Compact(indices)

	return model.FieldCapability{
		Type:          cap1.Type,
		Aggregatable:  cap1.Aggregatable && cap2.Aggregatable,
		Searchable:    cap1.Searchable && cap2.Searchable,
		MetadataField: util.Pointer(orFalse(cap1.MetadataField) && orFalse(cap2.MetadataField)),
		Indices:       indices,
	}, true
}

func orFalse(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}
