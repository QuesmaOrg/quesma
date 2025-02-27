// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"github.com/QuesmaOrg/quesma/platform/elasticsearch/elasticsearch_field_types"
	"github.com/QuesmaOrg/quesma/platform/schema"
)

type SchemaTypeAdapter struct {
}

func (e SchemaTypeAdapter) Convert(s string) (schema.QuesmaType, bool) {
	switch s {
	case elasticsearch_field_types.FieldTypeText:
		return schema.QuesmaTypeText, true
	case elasticsearch_field_types.FieldTypeKeyword:
		return schema.QuesmaTypeKeyword, true
	case elasticsearch_field_types.FieldTypeLong:
		return schema.QuesmaTypeLong, true
	case elasticsearch_field_types.FieldTypeDate:
		return schema.QuesmaTypeDate, true
	case elasticsearch_field_types.FieldTypeDateNanos:
		return schema.QuesmaTypeDate, true
	case elasticsearch_field_types.FieldTypeDouble:
		return schema.QuesmaTypeFloat, true
	case elasticsearch_field_types.FieldTypeBoolean:
		return schema.QuesmaTypeBoolean, true
	case elasticsearch_field_types.FieldTypeIp:
		return schema.QuesmaTypeIp, true
	case elasticsearch_field_types.FieldTypeGeoPoint:
		return schema.QuesmaTypePoint, true
	default:
		return schema.QuesmaTypeUnknown, false
	}
}
