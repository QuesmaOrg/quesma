package schemaregistry

type (
	SchemaRegistry interface {
		AllSchemas() map[TableName]Schema
		FindSchema(name TableName) (Schema, bool)
	}
	schemaRegistry struct {
		schemas map[TableName]Schema
	}
)

func (s *schemaRegistry) AllSchemas() map[TableName]Schema {
	return s.schemas
}

func (s *schemaRegistry) FindSchema(name TableName) (Schema, bool) {
	schema, found := s.schemas[name]
	return schema, found
}

func NewSchemaRegistry() SchemaRegistry {
	return &schemaRegistry{
		schemas: make(map[TableName]Schema),
	}
}
