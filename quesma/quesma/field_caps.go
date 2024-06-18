package quesma

import (
	"context"
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/plugins/registry"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/schema"
	"mitmproxy/quesma/util"
)

func BuildFieldCapFromSchema(fieldType schema.Type, indexName string) model.FieldCapability {
	return model.FieldCapability{
		Type:          elasticsearch.SchemaTypeAdapter{}.ConvertFrom(fieldType),
		Aggregatable:  fieldType.IsAggregatable(),
		Searchable:    fieldType.IsSearchable(),
		Indices:       []string{indexName},
		MetadataField: util.Pointer(false),
	}
}

func addFieldCapabilityFromSchemaRegistry(fields map[string]map[string]model.FieldCapability, colName string, fieldType schema.Type, index string) {
	fieldTypeName := elasticsearch.SchemaTypeAdapter{}.ConvertFrom(fieldType)
	fieldCapability := BuildFieldCapFromSchema(fieldType, index)

	if _, exists := fields[colName]; !exists {
		fields[colName] = make(map[string]model.FieldCapability)
	}

	if existing, exists := fields[colName][fieldTypeName]; exists {
		merged, ok := existing.Concat(fieldCapability)
		if ok {
			fields[colName][fieldTypeName] = merged
		}
	} else {
		fields[colName][fieldTypeName] = fieldCapability
	}
}

func handleFieldCapsIndex(cfg config.QuesmaConfiguration, schemaRegistry schema.Registry, indexes []string) ([]byte, error) {
	fields := make(map[string]map[string]model.FieldCapability)
	for _, resolvedIndex := range indexes {
		if len(resolvedIndex) == 0 {
			continue
		}

		if schemaDefinition, found := schemaRegistry.FindSchema(schema.TableName(resolvedIndex)); found {
			indexConfig, configured := cfg.IndexConfig[resolvedIndex]
			if configured && !indexConfig.Enabled {
				continue
			}

			fieldsWithAliases := make(map[schema.FieldName]schema.Field)
			for name, field := range schemaDefinition.Fields {
				fieldsWithAliases[name] = field
			}
			for name, aliasName := range schemaDefinition.Aliases {
				if field, exists := schemaDefinition.Fields[aliasName]; exists {
					fieldsWithAliases[name] = field
				}
			}
			for fieldName, field := range fieldsWithAliases {
				addFieldCapabilityFromSchemaRegistry(fields, fieldName.AsString(), field.Type, resolvedIndex)
				switch field.Type.Name {
				case "text":
					addFieldCapabilityFromSchemaRegistry(fields, fmt.Sprintf("%s.keyword", fieldName.AsString()), schema.TypeKeyword, resolvedIndex)
				case "keyword":
					addFieldCapabilityFromSchemaRegistry(fields, fmt.Sprintf("%s.text", fieldName.AsString()), schema.TypeText, resolvedIndex)
				}
			}
			transformer := registry.FieldCapsTransformerFor(resolvedIndex, cfg)
			var err error
			fields, err = transformer.Transform(fields)

			if err != nil {
				return nil, err
			}
		} else {
			logger.Error().Msgf("no schema found for index %s", resolvedIndex)
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

	return handleFieldCapsIndex(cfg, schemaRegistry, indexes)
}
