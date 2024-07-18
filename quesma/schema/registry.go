// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

import (
	"quesma/logger"
	"quesma/quesma/config"
	"strings"
)

type (
	Registry interface {
		AllSchemas() map[TableName]Schema
		FindSchema(name TableName) (Schema, bool)
	}
	schemaRegistry struct {
		configuration           config.QuesmaConfiguration
		dataSourceTableProvider TableProvider
		dataSourceTypeAdapter   typeAdapter
	}
	typeAdapter interface {
		Convert(string) (Type, bool)
	}
	TableProvider interface {
		TableDefinitions() map[string]Table
	}
	Table struct {
		Columns map[string]Column
	}
	Column struct {
		Name string
		Type string
	}
)

func (s *schemaRegistry) loadSchemas() (map[TableName]Schema, error) {
	definitions := s.dataSourceTableProvider.TableDefinitions()
	schemas := make(map[TableName]Schema)

	for indexName, indexConfiguration := range s.configuration.IndexConfig {
		fields := make(map[FieldName]Field)
		aliases := make(map[FieldName]FieldName)

		s.populateSchemaFromStaticConfiguration(indexConfiguration, fields)
		existsInDataSource := s.populateSchemaFromTableDefinition(definitions, indexName, fields)
		s.populateAliases(indexConfiguration, fields, aliases)
		schemas[TableName(indexName)] = NewSchemaWithAliases(fields, aliases, existsInDataSource)
	}

	return schemas, nil
}

func deprecatedConfigInUse(indexConfig config.IndexConfiguration) bool {
	return indexConfig.SchemaConfiguration == nil
}

func (s *schemaRegistry) AllSchemas() map[TableName]Schema {
	if schemas, err := s.loadSchemas(); err != nil {
		logger.Error().Msgf("error loading schemas: %v", err)
		return make(map[TableName]Schema)
	} else {
		return schemas
	}
}

func (s *schemaRegistry) FindSchema(name TableName) (Schema, bool) {
	if schemas, err := s.loadSchemas(); err != nil {
		logger.Error().Msgf("error loading schemas: %v", err)
		return Schema{}, false
	} else {
		schema, found := schemas[name]
		return schema, found
	}
}

func NewSchemaRegistry(tableProvider TableProvider, configuration config.QuesmaConfiguration, dataSourceTypeAdapter typeAdapter) Registry {
	return &schemaRegistry{
		configuration:           configuration,
		dataSourceTableProvider: tableProvider,
		dataSourceTypeAdapter:   dataSourceTypeAdapter,
	}
}

func (s *schemaRegistry) populateSchemaFromStaticConfiguration(indexConfiguration config.IndexConfiguration, fields map[FieldName]Field) {
	if deprecatedConfigInUse(indexConfiguration) {
		for fieldName, fieldType := range indexConfiguration.TypeMappings {
			if resolvedType, valid := ParseType(fieldType); valid {
				if resolvedType.Equal(TypePoint) {
					// TODO replace with notion of ephemeral types
					fields[FieldName(fieldName)] = Field{PropertyName: FieldName(fieldName), InternalPropertyName: FieldName(strings.Replace(fieldName, ".", "::", -1)), Type: resolvedType}
				} else {
					fields[FieldName(fieldName)] = Field{PropertyName: FieldName(fieldName), InternalPropertyName: FieldName(fieldName), Type: resolvedType}
				}
			} else {
				logger.Warn().Msgf("invalid configuration: type %s not supported (should have been spotted when validating configuration)", fieldType)
			}
		}
	} else {
		for _, field := range indexConfiguration.SchemaConfiguration.Fields {
			if field.Type.AsString() == config.TypeAlias {
				continue
			}
			if resolvedType, valid := ParseType(field.Type.AsString()); valid {
				// TODO replace with notion of ephemeral types
				if resolvedType.Equal(TypePoint) {
					fields[FieldName(field.Name.AsString())] = Field{PropertyName: FieldName(field.Name.AsString()), InternalPropertyName: FieldName(strings.Replace(field.Name.AsString(), ".", "::", -1)), Type: resolvedType}
				} else {
					fields[FieldName(field.Name.AsString())] = Field{PropertyName: FieldName(field.Name.AsString()), InternalPropertyName: FieldName(field.Name.AsString()), Type: resolvedType}
				}
			} else {
				logger.Warn().Msgf("invalid configuration: type %s not supported (should have been spotted when validating configuration)", field.Type.AsString())
			}
		}
	}
}

func (s *schemaRegistry) populateAliases(indexConfiguration config.IndexConfiguration, fields map[FieldName]Field, aliases map[FieldName]FieldName) {
	if deprecatedConfigInUse(indexConfiguration) {
		for aliasName, aliasConfig := range indexConfiguration.Aliases {
			if _, exists := fields[FieldName(aliasConfig.TargetFieldName)]; exists {
				aliases[FieldName(aliasName)] = FieldName(aliasConfig.TargetFieldName)
			} else {
				logger.Debug().Msgf("alias field %s not found, possibly not yet loaded", aliasConfig.SourceFieldName)
			}
		}
	} else {
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

func (s *schemaRegistry) populateSchemaFromTableDefinition(definitions map[string]Table, indexName string, fields map[FieldName]Field) (existsInDataSource bool) {
	tableDefinition, found := definitions[indexName]
	if found {
		logger.Debug().Msgf("loading schema for table %s", indexName)

		for _, column := range tableDefinition.Columns {
			propertyName := FieldName(strings.Replace(column.Name, "::", ".", -1))
			if existing, exists := fields[propertyName]; !exists {
				if quesmaType, resolved := s.dataSourceTypeAdapter.Convert(column.Type); resolved {
					fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), Type: quesmaType}
				} else {
					logger.Debug().Msgf("type %s not supported, falling back to text", column.Type)
					fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), Type: TypeText}
				}
			} else {
				fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), Type: existing.Type}
			}
		}
	}
	return found
}
