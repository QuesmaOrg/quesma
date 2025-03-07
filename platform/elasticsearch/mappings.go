// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"github.com/QuesmaOrg/quesma/platform/elasticsearch/elasticsearch_field_types"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"maps"
)

func ParseMappings(namespace string, mappings map[string]interface{}) map[string]schema.Column {
	result := make(map[string]schema.Column)

	properties, found := mappings["properties"]
	if !found {
		logger.Warn().Msgf("No 'properties' found in the mapping. The mapping was: %v", mappings)
		return result
	}

	for field, fieldMapping := range properties.(map[string]interface{}) {
		fieldMappingAsMap := fieldMapping.(map[string]interface{})

		var fieldName string
		if namespace != "" {
			fieldName = namespace + "." + field
		} else {
			fieldName = field
		}

		if typeMapping := fieldMappingAsMap["type"]; typeMapping != nil {
			parsedType, _ := ParseElasticType(typeMapping.(string))
			if parsedType.Name == schema.QuesmaTypeUnknown.Name {
				logger.Warn().Msgf("unknown type '%v' of field %s", typeMapping, fieldName)
			}
			result[fieldName] = schema.Column{Name: fieldName, Type: parsedType.Name}
		} else if fieldMappingAsMap["properties"] != nil {
			// Nested field
			maps.Copy(result, ParseMappings(fieldName, fieldMappingAsMap))
		} else {
			logger.Warn().Msgf("Unsupported type of field %s. Skipping the field. Full mapping: %v", fieldName, fieldMapping)
		}
	}
	return result
}

func GenerateMappings(schemaNode *schema.SchemaTreeNode) map[string]any {
	if schemaNode.Field != nil {
		result := map[string]any{"type": schemaTypeToElasticType(schemaNode.Field.Type)}
		if schemaNode.Field.Type.Name == schema.QuesmaTypeText.Name {
			result["fields"] = map[string]any{
				"keyword": map[string]any{"type": "keyword"},
			}
		}
		return result
	} else {
		result := make(map[string]any)
		for _, child := range schemaNode.Children {
			result[child.Name] = GenerateMappings(child)
		}
		return map[string]any{"properties": result}
	}
}

// FIXME: should be in elasticsearch_field_types, but this causes import cycle
func ParseElasticType(t string) (schema.QuesmaType, bool) {
	switch t {
	case elasticsearch_field_types.FieldTypeText:
		return schema.QuesmaTypeText, true
	case elasticsearch_field_types.FieldTypeKeyword:
		return schema.QuesmaTypeKeyword, true
	case elasticsearch_field_types.FieldTypeLong, elasticsearch_field_types.FieldTypeInteger, elasticsearch_field_types.FieldTypeShort, elasticsearch_field_types.FieldTypeByte:
		return schema.QuesmaTypeLong, true
	case elasticsearch_field_types.FieldTypeDate:
		return schema.QuesmaTypeTimestamp, true
	case elasticsearch_field_types.FieldTypeFloat, elasticsearch_field_types.FieldTypeHalfFloat, elasticsearch_field_types.FieldTypeDouble:
		return schema.QuesmaTypeFloat, true
	case elasticsearch_field_types.FieldTypeBoolean:
		return schema.QuesmaTypeBoolean, true
	case elasticsearch_field_types.FieldTypeIp:
		return schema.QuesmaTypeIp, true
	case elasticsearch_field_types.FieldTypeGeoPoint:
		return schema.QuesmaTypePoint, true
	case elasticsearch_field_types.FieldTypeObject:
		return schema.QuesmaTypeObject, true
	default:
		return schema.QuesmaTypeUnknown, false
	}
}

func schemaTypeToElasticType(t schema.QuesmaType) string {
	switch t.Name {
	case schema.QuesmaTypeText.Name:
		return elasticsearch_field_types.FieldTypeText
	case schema.QuesmaTypeKeyword.Name:
		return elasticsearch_field_types.FieldTypeKeyword
	case schema.QuesmaTypeInteger.Name:
		return elasticsearch_field_types.FieldTypeInteger
	case schema.QuesmaTypeLong.Name:
		return elasticsearch_field_types.FieldTypeLong
	case schema.QuesmaTypeUnsignedLong.Name:
		return elasticsearch_field_types.FieldTypeUnsignedLong
	case schema.QuesmaTypeTimestamp.Name:
		return elasticsearch_field_types.FieldTypeDate
	case schema.QuesmaTypeDate.Name:
		return elasticsearch_field_types.FieldTypeDate
	case schema.QuesmaTypeFloat.Name:
		return elasticsearch_field_types.FieldTypeDouble
	case schema.QuesmaTypeBoolean.Name:
		return elasticsearch_field_types.FieldTypeBoolean
	case schema.QuesmaTypeObject.Name:
		return elasticsearch_field_types.FieldTypeObject
	case schema.QuesmaTypeIp.Name:
		return elasticsearch_field_types.FieldTypeIp
	case schema.QuesmaTypePoint.Name:
		return elasticsearch_field_types.FieldTypeGeoPoint
	default:
		logger.Error().Msgf("Unknown Quesma type '%s', defaulting to 'text' type", t.Name)
		return elasticsearch_field_types.FieldTypeText
	}
}
