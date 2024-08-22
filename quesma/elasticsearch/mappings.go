// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"maps"
	"quesma/elasticsearch/elasticsearch_field_types"
	"quesma/logger"
	"quesma/schema"
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
			if parsedType.Name == schema.TypeUnknown.Name {
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
		if schemaNode.Field.Type.Name == schema.TypeText.Name {
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
func ParseElasticType(t string) (schema.Type, bool) {
	switch t {
	case elasticsearch_field_types.FieldTypeText:
		return schema.TypeText, true
	case elasticsearch_field_types.FieldTypeKeyword:
		return schema.TypeKeyword, true
	case elasticsearch_field_types.FieldTypeLong, elasticsearch_field_types.FieldTypeInteger, elasticsearch_field_types.FieldTypeShort, elasticsearch_field_types.FieldTypeByte:
		return schema.TypeLong, true
	case elasticsearch_field_types.FieldTypeDate:
		return schema.TypeTimestamp, true
	case elasticsearch_field_types.FieldTypeFloat, elasticsearch_field_types.FieldTypeHalfFloat, elasticsearch_field_types.FieldTypeDouble:
		return schema.TypeFloat, true
	case elasticsearch_field_types.FieldTypeBoolean:
		return schema.TypeBoolean, true
	case elasticsearch_field_types.FieldTypeIp:
		return schema.TypeIp, true
	case elasticsearch_field_types.FieldTypeGeoPoint:
		return schema.TypePoint, true
	default:
		return schema.TypeUnknown, false
	}
}

func schemaTypeToElasticType(t schema.Type) string {
	switch t.Name {
	case schema.TypeText.Name:
		return elasticsearch_field_types.FieldTypeText
	case schema.TypeKeyword.Name:
		return elasticsearch_field_types.FieldTypeKeyword
	case schema.TypeInteger.Name:
		return elasticsearch_field_types.FieldTypeInteger
	case schema.TypeLong.Name:
		return elasticsearch_field_types.FieldTypeLong
	case schema.TypeUnsignedLong.Name:
		return elasticsearch_field_types.FieldTypeUnsignedLong
	case schema.TypeTimestamp.Name:
		return elasticsearch_field_types.FieldTypeDate
	case schema.TypeDate.Name:
		return elasticsearch_field_types.FieldTypeDate
	case schema.TypeFloat.Name:
		return elasticsearch_field_types.FieldTypeDouble
	case schema.TypeBoolean.Name:
		return elasticsearch_field_types.FieldTypeBoolean
	case schema.TypeObject.Name:
		return elasticsearch_field_types.FieldTypeObject
	case schema.TypeIp.Name:
		return elasticsearch_field_types.FieldTypeIp
	case schema.TypePoint.Name:
		return elasticsearch_field_types.FieldTypeGeoPoint
	default:
		logger.Error().Msgf("Unknown type '%s', defaulting to 'text' type", t.Name)
		return elasticsearch_field_types.FieldTypeText
	}
}
