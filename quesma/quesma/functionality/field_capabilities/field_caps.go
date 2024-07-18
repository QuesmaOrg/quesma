// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package field_capabilities

import (
	"context"
	"encoding/json"
	"fmt"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/elasticsearch/elasticsearch_field_types"
	"quesma/logger"
	"quesma/model"
	"quesma/plugins/registry"
	"quesma/quesma/config"
	"quesma/quesma/errors"
	"quesma/schema"
	"quesma/util"
)

func addFieldCapabilityFromSchemaRegistry(fields map[string]map[string]model.FieldCapability, colName string, fieldType schema.Type, index string) {
	fieldTypeName := asElasticType(fieldType)
	fieldCapability := model.FieldCapability{
		Type:          asElasticType(fieldType),
		Aggregatable:  fieldType.IsAggregatable(),
		Searchable:    fieldType.IsSearchable(),
		Indices:       []string{index},
		MetadataField: util.Pointer(false),
	}

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
func makeSourceToDestMappings(indexMappings map[string]config.IndexMappingsConfiguration) map[string]string {
	sourceToDestMapping := make(map[string]string)
	for _, indexMapping := range indexMappings {
		for _, sourceIndex := range indexMapping.Mappings {
			destIndex := indexMapping.Name
			sourceToDestMapping[sourceIndex] = destIndex
		}
	}
	return sourceToDestMapping
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

func HandleFieldCaps(ctx context.Context, cfg config.QuesmaConfiguration, schemaRegistry schema.Registry, index string, lm *clickhouse.LogManager) ([]byte, error) {
	sourceToDestMapping := makeSourceToDestMappings(cfg.IndexSourceToInternalMappings)

	if destIndex, ok := sourceToDestMapping[index]; ok {
		index = destIndex
	}

	indexes, err := lm.ResolveIndexes(ctx, index)
	if err != nil {
		return nil, err
	}

	if len(indexes) == 0 {
		if !elasticsearch.IsIndexPattern(index) {
			return nil, quesma_errors.ErrIndexNotExists()
		}
	}

	return handleFieldCapsIndex(cfg, schemaRegistry, indexes)
}

func asElasticType(t schema.Type) string {
	switch t.Name {
	case schema.TypeText.Name:
		return elasticsearch_field_types.FieldTypeText
	case schema.TypeTimestamp.Name:
		return elasticsearch_field_types.FieldTypeDate
	case schema.TypeKeyword.Name:
		return elasticsearch_field_types.FieldTypeKeyword
	case schema.TypeLong.Name:
		return elasticsearch_field_types.FieldTypeLong
	case schema.TypeDate.Name:
		return elasticsearch_field_types.FieldTypeDate
	case schema.TypeFloat.Name:
		return elasticsearch_field_types.FieldTypeDouble
	case schema.TypeBoolean.Name:
		return elasticsearch_field_types.FieldTypeBoolean
	case schema.TypeIp.Name:
		return elasticsearch_field_types.FieldTypeIp
	case schema.TypeObject.Name:
		return elasticsearch_field_types.FieldTypeObject
	case schema.TypePoint.Name:
		return elasticsearch_field_types.FieldTypeGeoPoint
	default:
		return elasticsearch_field_types.FieldTypeText
	}
}
