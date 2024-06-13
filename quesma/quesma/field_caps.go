package quesma

import (
	"context"
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/elasticsearch/elasticsearch_field_types"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/schema"
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

func BuildFieldCapFromSchema(fieldType schema.Type, indexName string) model.FieldCapability {
	return model.FieldCapability{
		// TODO adapter needs to be moved to elasticsearch
		Type:         schema.ElasticsearchTypeAdapter{}.ConvertFrom(fieldType),
		Aggregatable: fieldType.IsAggregatable(),
		Searchable:   fieldType.IsSearchable(),
		Indices:      []string{indexName},
	}
}

func BuildFieldCapability(indexName, typeName string) model.FieldCapability {
	capability := model.FieldCapability{
		Type:         typeName,
		Aggregatable: elasticsearch_field_types.IsAggregatable(typeName),
		Searchable:   true,
		Indices:      []string{indexName},
	}
	if typeName != elasticsearch_field_types.FieldTypeKeyword {
		capability.MetadataField = util.Pointer(false)
	}
	return capability
}

func addFieldCapabilityFromSchemaRegistry(fields map[string]map[string]model.FieldCapability, colName string, fieldType schema.Type, index string) {
	fieldTypeName := schema.ElasticsearchTypeAdapter{}.ConvertFrom(fieldType)
	fieldCapability := BuildFieldCapFromSchema(fieldType, index)

	if _, exists := fields[colName]; !exists {
		fields[colName] = make(map[string]model.FieldCapability)
	}

	if existing, exists := fields[colName][fieldTypeName]; exists {
		merged, ok := merge(existing, fieldCapability)
		if ok {
			fields[colName][fieldTypeName] = merged
		}
	} else {
		fields[colName][fieldTypeName] = fieldCapability
	}
}

func addFieldCapabilityFromStaticSchema(fields map[string]map[string]model.FieldCapability, colName string, typeName string, index string) {
	fieldCapability := BuildFieldCapability(index, typeName)

	if _, exists := fields[colName]; !exists {
		fields[colName] = make(map[string]model.FieldCapability)
	}

	if existing, exists := fields[colName][typeName]; exists {
		merged, ok := merge(existing, fieldCapability)
		if ok {
			fields[colName][typeName] = merged
		}
	} else {
		fields[colName][typeName] = fieldCapability
	}
}

func addNewDefaultFieldCapability(fields map[string]map[string]model.FieldCapability, col *clickhouse.Column, index string) {
	typeName := mapClickhouseToElasticType(col)
	fieldCapability := BuildFieldCapability(index, typeName)

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

func isConfiguredExplicitly(indexName string, fieldName config.FieldName, cfg config.QuesmaConfiguration) (string, bool) {
	if indexConfig, exists := cfg.IndexConfig[indexName]; exists {
		if indexConfig.TypeMappings != nil {
			if fieldConfig, exists := indexConfig.TypeMappings[fieldName.AsString()]; exists {
				return fieldConfig, exists
			}
		}
	}
	return "", false
}

func canBeKeywordField(col *clickhouse.Column) bool {
	typeName := mapClickhouseToElasticType(col)
	return typeName == "text" || typeName == "LowCardinality(String)"
}

func addNewKeywordFieldCapability(fields map[string]map[string]model.FieldCapability, col *clickhouse.Column, index string) {
	var keyword = BuildFieldCapability(index, elasticsearch_field_types.FieldTypeKeyword)
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

func handleFieldCapsIndex(ctx context.Context, cfg config.QuesmaConfiguration, schemaRegistry schema.Registry, indexes []string, tables clickhouse.TableMap) ([]byte, error) {
	fields := make(map[string]map[string]model.FieldCapability)
	for _, resolvedIndex := range indexes {
		if len(resolvedIndex) == 0 {
			continue
		}

		if schema, found := schemaRegistry.FindSchema(schema.TableName(resolvedIndex)); found {
			logger.Info().Msgf("found schema for index %s", resolvedIndex)

			for fieldName, field := range schema.Fields {
				logger.Info().Msgf("field: %s, type: %s", fieldName, field.Type)

				addFieldCapabilityFromSchemaRegistry(fields, fieldName.AsString(), field.Type, resolvedIndex)
			}
		} else {
			logger.Info().Msgf("no schema found for index %s", resolvedIndex)
		}

		if table, ok := tables.Load(resolvedIndex); ok {
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

func handleFieldCaps(ctx context.Context, cfg config.QuesmaConfiguration, schemaRegistry schema.Registry, index string, lm *clickhouse.LogManager) ([]byte, error) {
	indexes, err := lm.ResolveIndexes(ctx, index)
	if err != nil {
		return nil, err
	}

	if len(indexes) == 0 {
		if !elasticsearch.IsIndexPattern(index) {
			return nil, errIndexNotExists
		}
	}

	tables, err := lm.GetTableDefinitions()
	if err != nil {
		return nil, err
	}

	return handleFieldCapsIndex(ctx, cfg, schemaRegistry, indexes, tables)
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
