package schemaregistry

type (
	Schema struct {
		Fields map[FieldName]Field
	}
	Field struct {
		Name FieldName
		Type Type
	}
	TableName string
	FieldName string
	Type      string
)
