// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"github.com/rs/zerolog/log"
	"maps"
	"quesma/elasticsearch/elasticsearch_field_types"
	"quesma/schema"
)

func ParseMappings(namespace string, mappings map[string]interface{}) map[string]schema.Column {
	result := make(map[string]schema.Column)

	properties, found := mappings["properties"]
	if !found {
		log.Warn().Msgf("No 'properties' found in the mapping. The mapping was: %v", mappings)
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
			parsedType, _ := parseElasticType(typeMapping.(string))
			if parsedType.Name == schema.TypeUnknown.Name {
				log.Warn().Msgf("unknown type '%v' of field %s", typeMapping, fieldName)
			}
			result[fieldName] = schema.Column{Name: fieldName, Type: parsedType.Name}
		} else if fieldMappingAsMap["properties"] != nil {
			// Nested field
			maps.Copy(result, ParseMappings(fieldName, fieldMappingAsMap))
		} else {
			log.Warn().Msgf("Unsupported type of field %s. Skipping the field. Full mapping: %v", fieldName, fieldMapping)
		}
	}
	return result
}

// FIXME: should be in elasticsearch_field_types, but this causes import cycle
func parseElasticType(t string) (schema.Type, bool) {
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
