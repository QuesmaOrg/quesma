package elasticsearch

import (
	"mitmproxy/quesma/elasticsearch/elasticsearch_field_types"
	"mitmproxy/quesma/schema"
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
	default:
		return schema.TypeUnknown, false
	}
}

func (e SchemaTypeAdapter) ConvertFrom(t schema.Type) string {
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
	default:
		return elasticsearch_field_types.FieldTypeText
	}
}
