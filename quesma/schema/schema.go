package schema

import (
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"sync/atomic"
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
	Registry interface {
		AllSchemas() map[TableName]Schema
		FindSchema(name TableName) (Schema, bool)
		Load() error
		Start()
	}
	schemaRegistry struct {
		started                atomic.Bool
		schemas                *concurrent.Map[TableName, Schema]
		configuration          config.QuesmaConfiguration
		clickhouseSchemaLoader *clickhouse.SchemaLoader
		ClickhouseTypeAdapter  ClickhouseTypeAdapter
	}
)

func (s *schemaRegistry) Start() {
	if s.started.CompareAndSwap(false, true) {
		s.loadTypeMappingsFromConfiguration()
		if err := s.Load(); err != nil {
			logger.Error().Msgf("error loading schemas: %v", err)
		}
	}
}

func (s *schemaRegistry) loadTypeMappingsFromConfiguration() {
	for _, indexConfiguration := range s.configuration.IndexConfig {
		if !indexConfiguration.Enabled {
			continue
		}
		if indexConfiguration.SchemaConfiguration != nil {
			logger.Debug().Msgf("loading schema for index %s", indexConfiguration.Name)
			fields := make(map[FieldName]Field)
			for _, field := range indexConfiguration.SchemaConfiguration.Fields {
				fieldName := FieldName(field.Name)
				if resolvedType, valid := IsValid(field.Type.AsString()); valid {
					fields[fieldName] = Field{
						Name: fieldName,
						Type: resolvedType,
					}
				} else {
					logger.Error().Msgf("invalid configuration: type %s not supported (should have been spotted when validating configuration)", field.Type)
				}
			}
			s.schemas.Store(TableName(indexConfiguration.Name), Schema{Fields: fields})
		}
	}
}

func (s *schemaRegistry) Load() error {
	if !s.started.Load() {
		return fmt.Errorf("schema registry not started")
	}
	// refreshed periodically by LogManager
	definitions := s.clickhouseSchemaLoader.TableDefinitions()
	schemas := s.schemas.Snapshot()
	definitions.Range(func(indexName string, value *clickhouse.Table) bool {
		logger.Debug().Msgf("loading schema for table %s", indexName)
		fields := make(map[FieldName]Field)
		if schema, found := schemas[TableName(indexName)]; found {
			fields = schema.Fields
		}
		for _, col := range value.Cols {
			indexConfig := s.configuration.IndexConfig[indexName]
			if explicitType, found := indexConfig.TypeMappings[col.Name]; found {
				logger.Debug().Msgf("found explicit type mapping for column %s: %s", col.Name, explicitType)
				fields[FieldName(col.Name)] = Field{
					Name: FieldName(col.Name),
					Type: Type(explicitType),
				}
				continue
			}
			if _, exists := fields[FieldName(col.Name)]; !exists {
				if quesmaType, found := s.ClickhouseTypeAdapter.Adapt(col.Type.String()); found {
					fields[FieldName(col.Name)] = Field{
						Name: FieldName(col.Name),
						Type: quesmaType,
					}
				} else {
					logger.Error().Msgf("type %s not supported", col.Type.String())
				}
			}
		}
		s.schemas.Store(TableName(indexName), Schema{Fields: fields})
		return true
	})
	for name, schema := range s.schemas.Snapshot() {
		logger.Debug().Msgf("schema: %s", name)
		for fieldName, field := range schema.Fields {
			logger.Debug().Msgf("\tfield: %s, type: %s", fieldName, field.Type)
		}

		break
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

func NewSchemaRegistry(schemaManagement *clickhouse.SchemaLoader, configuration config.QuesmaConfiguration) Registry {
	return &schemaRegistry{
		schemas:                concurrent.NewMap[TableName, Schema](),
		started:                atomic.Bool{},
		configuration:          configuration,
		clickhouseSchemaLoader: schemaManagement,
		ClickhouseTypeAdapter:  NewClickhouseTypeAdapter(),
	}
}
