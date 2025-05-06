// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package field_capabilities

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch/elasticsearch_field_types"
	"github.com/QuesmaOrg/quesma/platform/errors"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/goccy/go-json"
)

func addFieldCapabilityFromSchemaRegistry(fields map[string]map[string]model.FieldCapability, colName string, fieldType schema.QuesmaType, index string) {
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

func handleFieldCapsIndex(cfg map[string]config.IndexConfiguration, schemaRegistry schema.Registry, indexes []string) ([]byte, error) {
	fields := make(map[string]map[string]model.FieldCapability)

	schemas := schemaRegistry.AllSchemas()

	for _, resolvedIndex := range indexes {
		if len(resolvedIndex) == 0 {
			continue
		}

		if schemaDefinition, found := schemas[schema.IndexName(resolvedIndex)]; found {
			indexConfig, configured := cfg[resolvedIndex]
			if configured && !indexConfig.IsClickhouseQueryEnabled() {
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
				case "keyword", "text":
					addFieldCapabilityFromSchemaRegistry(fields, fmt.Sprintf("%s%s", fieldName.AsString(), types.MultifieldKeywordSuffix), schema.QuesmaTypeKeyword, resolvedIndex)
					addFieldCapabilityFromSchemaRegistry(fields, fmt.Sprintf("%s%s", fieldName.AsString(), types.MultifieldTextSuffix), schema.QuesmaTypeText, resolvedIndex)
				}
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

func HandleFieldCaps(ctx context.Context, cfg map[string]config.IndexConfiguration, schemaRegistry schema.Registry, index string, lm clickhouse.LogManagerIFace) ([]byte, error) {
	indexes, err := lm.ResolveIndexPattern(ctx, schemaRegistry, index)
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

func asElasticType(t schema.QuesmaType) string {
	switch t.Name {
	case schema.QuesmaTypeText.Name:
		return elasticsearch_field_types.FieldTypeText
	case schema.QuesmaTypeTimestamp.Name:
		return elasticsearch_field_types.FieldTypeDate
	case schema.QuesmaTypeKeyword.Name:
		return elasticsearch_field_types.FieldTypeKeyword
	case schema.QuesmaTypeLong.Name:
		return elasticsearch_field_types.FieldTypeLong
	case schema.QuesmaTypeDate.Name:
		return elasticsearch_field_types.FieldTypeDate
	case schema.QuesmaTypeFloat.Name:
		return elasticsearch_field_types.FieldTypeDouble
	case schema.QuesmaTypeBoolean.Name:
		return elasticsearch_field_types.FieldTypeBoolean
	case schema.QuesmaTypeIp.Name:
		return elasticsearch_field_types.FieldTypeIp
	case schema.QuesmaTypeObject.Name:
		return elasticsearch_field_types.FieldTypeObject
	case schema.QuesmaTypePoint.Name:
		return elasticsearch_field_types.FieldTypeGeoPoint
	case schema.QuesmaTypeInteger.Name:
		return elasticsearch_field_types.FieldTypeInteger
	case schema.QuesmaTypeMap.Name:
		return elasticsearch_field_types.FieldTypeObject
	default:
		return elasticsearch_field_types.FieldTypeText
	}
}
