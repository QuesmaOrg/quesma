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
	Registry interface {
		AllSchemas() map[TableName]Schema
		FindSchema(name TableName) (Schema, bool)
		Start()
	}
	schemaRegistry struct {
		started               atomic.Bool
		schemas               *concurrent.Map[TableName, Schema]
		configuration         config.QuesmaConfiguration
		chTableDiscovery      clickhouse.TableDiscovery
		dataSourceTypeAdapter TypeAdapter
		connectorTypeAdapter  TypeAdapter
	}
	TypeAdapter interface {
		Convert(string) (Type, bool)
	}
)

func (s *schemaRegistry) Start() {
	if s.started.CompareAndSwap(false, true) {
		s.loadTypeMappingsFromConfiguration()
		if err := s.Load(); err != nil {
			logger.Error().Msgf("error loading schemas: %v", err)
		}

		for name, schema := range s.schemas.Snapshot() {
			logger.Debug().Msgf("schema: %s", name)
			for fieldName, field := range schema.Fields {
				logger.Debug().Msgf("field: %s, type: %s", fieldName, field.Type)
			}
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
				} else if field.Type.AsString() != config.TypeAlias {
					logger.Warn().Msgf("invalid configuration: type %s not supported (should have been spotted when validating configuration)", field.Type)
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
	definitions := s.chTableDiscovery.TableDefinitions()
	schemas := s.schemas.Snapshot()
	definitions.Range(func(indexName string, value *clickhouse.Table) bool {
		logger.Debug().Msgf("loading schema for table %s", indexName)
		fields := make(map[FieldName]Field)
		aliases := make(map[FieldName]FieldName)
		if schema, found := schemas[TableName(indexName)]; found {
			fields = schema.Fields
		}
		for _, col := range value.Cols {
			indexConfig := s.configuration.IndexConfig[indexName]

			// TODO replace with dedicated schema config
			if indexConfig.SchemaConfiguration == nil {
				logger.Debug().Msgf("using deprecated type mappings for index %s", indexName)
				if explicitType, found := indexConfig.TypeMappings[col.Name]; found {
					if resolvedQuesmaType, found := s.connectorTypeAdapter.Convert(explicitType); found {
						logger.Debug().Msgf("found explicit type mapping for column %s: %s", col.Name, resolvedQuesmaType)
						fields[FieldName(col.Name)] = Field{
							Name: FieldName(col.Name),
							Type: resolvedQuesmaType,
						}
						continue
					} else {
						// TODO those will need to be validated at config stage
						logger.Error().Msgf("type %s not supported", explicitType)
					}
				}
			} else {
				logger.Debug().Msgf("using schema configuration for index %s", indexName)
				if fieldConfiguration, found := indexConfig.SchemaConfiguration.Fields[config.FieldName(col.Name)]; found {
					if resolvedQuesmaType, found := s.connectorTypeAdapter.Convert(fieldConfiguration.Type.AsString()); found {
						logger.Debug().Msgf("found explicit type mapping for column %s: %s", col.Name, resolvedQuesmaType)
						fields[FieldName(col.Name)] = Field{
							Name: FieldName(col.Name),
							Type: resolvedQuesmaType,
						}
						continue
					} else {
						// TODO those will need to be validated at config stage
						logger.Error().Msgf("type %s not supported", fieldConfiguration)
					}
				}
			}

			if _, exists := fields[FieldName(col.Name)]; !exists {
				if quesmaType, found := s.dataSourceTypeAdapter.Convert(col.Type.String()); found {
					fields[FieldName(col.Name)] = Field{
						Name: FieldName(col.Name),
						Type: quesmaType,
					}
				} else {
					logger.Debug().Msgf("type %s not supported, falling back to text", col.Type.String())
					fields[FieldName(col.Name)] = Field{
						Name: FieldName(col.Name),
						Type: TypeText,
					}
				}
			}
		}

		for _, indexConfiguration := range s.configuration.IndexConfig {
			if !indexConfiguration.Enabled {
				continue
			}
			if indexConfiguration.SchemaConfiguration != nil {
				for _, field := range indexConfiguration.SchemaConfiguration.Fields {
					if field.Type.AsString() == config.TypeAlias {
						if _, exists := fields[FieldName(field.AliasedField)]; exists {
							aliases[FieldName(field.Name)] = FieldName(field.AliasedField)
						} else {
							logger.Debug().Msgf("alias field %s not found, possibly not yet loaded", field.AliasedField)
						}
					}
				}
			}
		}

		s.schemas.Store(TableName(indexName), Schema{Fields: fields, Aliases: aliases})
		return true
	})
	for name, schema := range s.schemas.Snapshot() {
		logger.Debug().Msgf("schema: %s", name)
		for fieldName, field := range schema.Fields {
			logger.Debug().Msgf("\tfield: %s, type: %s", fieldName, field.Type)
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

func NewSchemaRegistry(chTableDiscovery clickhouse.TableDiscovery, configuration config.QuesmaConfiguration, dataSourceTypeAdapter, connectorTypeAdapter TypeAdapter) Registry {
	return &schemaRegistry{
		schemas:               concurrent.NewMap[TableName, Schema](),
		started:               atomic.Bool{},
		configuration:         configuration,
		chTableDiscovery:      chTableDiscovery,
		dataSourceTypeAdapter: dataSourceTypeAdapter,
		connectorTypeAdapter:  connectorTypeAdapter,
	}
}
