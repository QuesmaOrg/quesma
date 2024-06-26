// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"quesma/elasticsearch/elasticsearch_field_types"
	"quesma/schema"
)

type SchemaTypeAdapter struct {
}

func (e SchemaTypeAdapter) Convert(s string) (schema.Type, bool) {
	switch s {
	case elasticsearch_field_types.FieldTypeText:
		return schema.TypeText, true
	case elasticsearch_field_types.FieldTypeKeyword:
		return schema.TypeKeyword, true
	case elasticsearch_field_types.FieldTypeLong:
		return schema.TypeLong, true
	case elasticsearch_field_types.FieldTypeDate:
		return schema.TypeDate, true
	case elasticsearch_field_types.FieldTypeDateNanos:
		return schema.TypeDate, true
	case elasticsearch_field_types.FieldTypeDouble:
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
