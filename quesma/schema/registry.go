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
		UpdateDynamicConfiguration(name TableName, table Table)
	}
	schemaRegistry struct {
		// index configuration overrides always take precedence
		indexConfiguration      *map[string]config.IndexConfiguration
		dataSourceTableProvider TableProvider
		dataSourceTypeAdapter   typeAdapter
		dynamicConfiguration    map[string]Table
	}
	typeAdapter interface {
		Convert(string) (QuesmaType, bool)
	}
	TableProvider interface {
		TableDefinitions() map[string]Table
		AutodiscoveryEnabled() bool
	}
	Table struct {
		Columns      map[string]Column
		DatabaseName string
	}
	Column struct {
		Name string
		Type string // FIXME: change to schema.Type
	}
)

func (s *schemaRegistry) loadSchemas() (map[TableName]Schema, error) {
	definitions := s.dataSourceTableProvider.TableDefinitions()
	schemas := make(map[TableName]Schema)
	if s.dataSourceTableProvider.AutodiscoveryEnabled() {
		for tableName, table := range definitions {
			fields := make(map[FieldName]Field)
			existsInDataSource := s.populateSchemaFromTableDefinition(definitions, tableName, fields)
			schemas[TableName(tableName)] = NewSchema(fields, existsInDataSource, table.DatabaseName)
		}
		return schemas, nil
	}

	for indexName, indexConfiguration := range *s.indexConfiguration {
		fields := make(map[FieldName]Field)
		aliases := make(map[FieldName]FieldName)

		s.populateSchemaFromDynamicConfiguration(indexName, fields)
		s.populateSchemaFromStaticConfiguration(indexConfiguration, fields)
		existsInDataSource := s.populateSchemaFromTableDefinition(definitions, indexName, fields)
		s.populateAliases(indexConfiguration, fields, aliases)
		s.removeIgnoredFields(indexConfiguration, fields, aliases)
		s.removeGeoPhysicalFields(fields)
		if tableDefinition, ok := definitions[indexName]; ok {
			schemas[TableName(indexName)] = NewSchemaWithAliases(fields, aliases, existsInDataSource, tableDefinition.DatabaseName)
		} else {
			schemas[TableName(indexName)] = NewSchemaWithAliases(fields, aliases, existsInDataSource, "")
		}
	}

	return schemas, nil
}

func (s *schemaRegistry) populateSchemaFromDynamicConfiguration(indexName string, fields map[FieldName]Field) {
	d, found := s.dynamicConfiguration[indexName]
	if !found {
		return
	}
	for _, column := range d.Columns {
		columnType, success := ParseQuesmaType(column.Type)
		if !success {
			logger.Warn().Msgf("Invalid dynamic configuration: type %s (of field %s in index %s) not supported. Skipping the field.", column.Type, column.Name, indexName)
			continue
		}

		fields[FieldName(column.Name)] = Field{PropertyName: FieldName(column.Name), InternalPropertyName: FieldName(column.Name), Type: columnType}
	}
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

func (s *schemaRegistry) UpdateDynamicConfiguration(name TableName, table Table) {
	s.dynamicConfiguration[name.AsString()] = table
}

func NewSchemaRegistry(tableProvider TableProvider, configuration *config.QuesmaConfiguration, dataSourceTypeAdapter typeAdapter) Registry {
	return &schemaRegistry{
		indexConfiguration:      &configuration.IndexConfig,
		dataSourceTableProvider: tableProvider,
		dataSourceTypeAdapter:   dataSourceTypeAdapter,
		dynamicConfiguration:    make(map[string]Table),
	}
}

func (s *schemaRegistry) populateSchemaFromStaticConfiguration(indexConfiguration config.IndexConfiguration, fields map[FieldName]Field) {
	if indexConfiguration.SchemaOverrides == nil {
		return
	}
	for fieldName, field := range indexConfiguration.SchemaOverrides.Fields {
		if field.Type.AsString() == config.TypeAlias || field.Ignored {
			continue
		}
		if resolvedType, valid := ParseQuesmaType(field.Type.AsString()); valid {
			fields[FieldName(fieldName)] = Field{PropertyName: FieldName(fieldName), InternalPropertyName: FieldName(strings.Replace(fieldName.AsString(), ".", "::", -1)), Type: resolvedType}
		} else {
			logger.Warn().Msgf("invalid configuration: type %s not supported (should have been spotted when validating configuration)", field.Type.AsString())
		}
	}
}

func (s *schemaRegistry) populateAliases(indexConfiguration config.IndexConfiguration, _ map[FieldName]Field, aliases map[FieldName]FieldName) {
	if indexConfiguration.SchemaOverrides == nil {
		return
	}
	for fieldName, fieldConf := range indexConfiguration.SchemaOverrides.Fields {
		if fieldConf.Type.AsString() == config.TypeAlias && fieldConf.TargetColumnName != "" {
			aliases[FieldName(fieldName)] = FieldName(fieldConf.TargetColumnName)
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
					fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), InternalPropertyType: column.Type, Type: quesmaType}
				} else {
					logger.Debug().Msgf("type %s not supported, falling back to keyword", column.Type)
					fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), InternalPropertyType: column.Type, Type: QuesmaTypeKeyword}
				}
			} else {
				fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), InternalPropertyType: column.Type, Type: existing.Type}
			}
		}
	}
	return found
}

func (s *schemaRegistry) removeIgnoredFields(indexConfiguration config.IndexConfiguration, fields map[FieldName]Field, aliases map[FieldName]FieldName) {
	if indexConfiguration.SchemaOverrides == nil {
		return
	}
	for fieldName, field := range indexConfiguration.SchemaOverrides.Fields {
		if field.Ignored {
			delete(fields, FieldName(fieldName))
			delete(aliases, FieldName(fieldName))
		}
	}
}

func (s *schemaRegistry) removeGeoPhysicalFields(fields map[FieldName]Field) {

	for fieldName, field := range fields {
		if field.Type.Name == QuesmaTypePoint.Name {
			// do not expose geo fields to the user, it's an internal representation
			delete(fields, fieldName+".lat")
			delete(fields, fieldName+".lon")
		}
	}
}
