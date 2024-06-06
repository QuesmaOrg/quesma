package schema

import (
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
)

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

type (
	SchemaRegistry interface {
		AllSchemas() map[TableName]Schema
		FindSchema(name TableName) (Schema, bool)
		Load() error
	}
	schemaRegistry struct {
		schemas                *concurrent.Map[TableName, Schema]
		configuration          config.QuesmaConfiguration
		clickhouseSchemaLoader *clickhouse.SchemaLoader
		ClickhouseTypeAdapter  ClickhouseTypeAdapter
	}
)

func (s *schemaRegistry) Load() error {
	definitions := s.clickhouseSchemaLoader.TableDefinitions()
	definitions.Range(func(indexName string, value *clickhouse.Table) bool {
		logger.Info().Msgf("loading schema for table %s", indexName)
		fields := make(map[FieldName]Field)
		for _, col := range value.Cols {
			indexConfig := s.configuration.IndexConfig[indexName]
			if explicitType, found := indexConfig.TypeMappings[col.Name]; found {
				logger.Info().Msgf("found explicit type mapping for column %s: %s", col.Name, explicitType)
				fields[FieldName(col.Name)] = Field{
					Name: FieldName(col.Name),
					Type: Type(explicitType),
				}
				continue
			}
			quesmaType, found := s.ClickhouseTypeAdapter.Adapt(col.Type.String())
			if !found {
				fmt.Printf("type %s not supported\n", col.Type.String())
				continue
			} else {
				fields[FieldName(col.Name)] = Field{
					Name: FieldName(col.Name),
					Type: quesmaType, // TODO convert to our type
				}
			}
		}
		s.schemas.Store(TableName(indexName), Schema{Fields: fields})
		return true
	})
	for name, schema := range s.schemas.Snapshot() {
		fmt.Printf("schema: %s\n", name)
		for fieldName, field := range schema.Fields {
			fmt.Printf("\tfield: %s, type: %s\n", fieldName, field.Type)
		}

	}
	return nil
}

func (s *schemaRegistry) AllSchemas() map[TableName]Schema {
	return s.schemas.Snapshot()
}

func (s *schemaRegistry) FindSchema(name TableName) (Schema, bool) {
	schema, found := s.schemas.Load(name)
	return schema, found
}

func NewSchemaRegistry(schemaManagement *clickhouse.SchemaLoader, configuration config.QuesmaConfiguration) SchemaRegistry {
	return &schemaRegistry{
		schemas:                concurrent.NewMap[TableName, Schema](),
		configuration:          configuration,
		clickhouseSchemaLoader: schemaManagement,
		ClickhouseTypeAdapter:  NewClickhouseTypeAdapter(),
	}
}
