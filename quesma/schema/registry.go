// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

import (
	"github.com/QuesmaOrg/quesma/quesma/comment_metadata"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"sync"
)

// TODO we should rethink naming and types used in this package

type (
	Registry interface {
		AllSchemas() map[IndexName]Schema
		FindSchema(name IndexName) (Schema, bool)
		UpdateFieldsOrigins(name IndexName, fields map[FieldName]FieldSource)
		UpdateDynamicConfiguration(name IndexName, table Table)
		UpdateFieldEncodings(encodings map[FieldEncodingKey]EncodedFieldName)
		GetFieldEncodings() map[FieldEncodingKey]EncodedFieldName
	}

	FieldEncodingKey struct {
		TableName string
		FieldName string
	}
	EncodedFieldName string
	schemaRegistry   struct {
		// index configuration overrides always take precedence
		indexConfiguration      *map[string]config.IndexConfiguration
		dataSourceTableProvider TableProvider
		dataSourceTypeAdapter   typeAdapter
		dynamicConfiguration    map[string]Table
		fieldEncodingsLock      sync.RWMutex
		fieldEncodings          map[FieldEncodingKey]EncodedFieldName
		fieldOriginsLock        sync.RWMutex
		fieldOrigins            map[IndexName]map[FieldName]FieldSource
	}
	typeAdapter interface {
		Convert(string) (QuesmaType, bool)
	}
	TableProvider interface {
		TableDefinitions() map[string]Table
		AutodiscoveryEnabled() bool
	}
	// TUTAJ
	Table struct {
		Columns      map[string]Column
		PrimaryKey   *string // nil if no primary key
		DatabaseName string
	}
	Column struct {
		Name    string
		Type    string // FIXME: change to schema.Type
		Comment string
	}
)

func (s *schemaRegistry) getInternalToPublicFieldEncodings(tableName string) map[EncodedFieldName]string {
	fieldsEncodingsPerIndex := make(map[string]EncodedFieldName)
	s.fieldEncodingsLock.RLock()
	for key, value := range s.fieldEncodings {
		if key.TableName == tableName {
			fieldsEncodingsPerIndex[key.FieldName] = EncodedFieldName(value)
		}
	}
	s.fieldEncodingsLock.RUnlock()
	internalToPublicFieldsEncodings := make(map[EncodedFieldName]string)

	for key, value := range fieldsEncodingsPerIndex {
		internalToPublicFieldsEncodings[value] = key
	}

	return internalToPublicFieldsEncodings
}

func (s *schemaRegistry) loadSchemas() (map[IndexName]Schema, error) {
	definitions := s.dataSourceTableProvider.TableDefinitions()
	schemas := make(map[IndexName]Schema)

	if s.dataSourceTableProvider.AutodiscoveryEnabled() {
		for tableName, table := range definitions {
			fields := make(map[FieldName]Field)
			internalToPublicFieldsEncodings := s.getInternalToPublicFieldEncodings(tableName)
			existsInDataSource := s.populateSchemaFromTableDefinition(definitions, tableName, fields, internalToPublicFieldsEncodings)
			schemas[IndexName(tableName)] = NewSchema(fields, existsInDataSource, table.DatabaseName, nil)
		}
	}

	for indexName, indexConfiguration := range *s.indexConfiguration {
		fields := make(map[FieldName]Field)
		aliases := make(map[FieldName]FieldName)
		s.populateSchemaFromDynamicConfiguration(indexName, fields)
		s.populateSchemaFromStaticConfiguration(indexConfiguration, fields)
		internalToPublicFieldsEncodings := s.getInternalToPublicFieldEncodings(indexName)
		tableName := indexConfiguration.TableName(indexName)
		existsInDataSource := s.populateSchemaFromTableDefinition(definitions, tableName, fields, internalToPublicFieldsEncodings)
		s.populateAliases(indexConfiguration, fields, aliases)
		s.removeIgnoredFields(indexConfiguration, fields, aliases)
		s.removeGeoPhysicalFields(fields)
		s.populateFieldsOrigins(indexName, fields)
		var primaryKey *FieldName
		if indexConfiguration.PrimaryKey != nil {
			primaryKey = new(FieldName)
			*primaryKey = FieldName(*indexConfiguration.PrimaryKey)
		}
		if tableDefinition, ok := definitions[indexName]; ok {
			schemas[IndexName(indexName)] = NewSchemaWithAliases(fields, aliases, existsInDataSource, tableDefinition.DatabaseName, primaryKey)
		} else {
			schemas[IndexName(indexName)] = NewSchemaWithAliases(fields, aliases, existsInDataSource, "", primaryKey)
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

		fields[FieldName(column.Name)] = Field{PropertyName: FieldName(column.Name), InternalPropertyName: FieldName(column.Name), Type: columnType, Origin: FieldSourceMapping}
	}
}

func (s *schemaRegistry) AllSchemas() map[IndexName]Schema {
	if schemas, err := s.loadSchemas(); err != nil {
		logger.Error().Msgf("error loading schemas: %v", err)
		return make(map[IndexName]Schema)
	} else {
		return schemas
	}
}

func (s *schemaRegistry) FindSchema(name IndexName) (Schema, bool) {
	if schemas, err := s.loadSchemas(); err != nil {
		logger.Error().Msgf("error loading schemas: %v", err)
		return Schema{}, false
	} else {
		schema, found := schemas[name]
		return schema, found
	}
}

func (s *schemaRegistry) UpdateDynamicConfiguration(name IndexName, table Table) {
	s.dynamicConfiguration[name.AsString()] = table
	dynamicEncodings := make(map[FieldEncodingKey]EncodedFieldName)
	for _, column := range table.Columns {
		// when table is created based on PUT `name/_mapping` we need to populate field encodings.
		// Otherwise, they will be populated only based on ingested data which might not contain all the fields
		dynamicEncodings[FieldEncodingKey{TableName: name.AsString(), FieldName: column.Name}] = EncodedFieldName(util.FieldToColumnEncoder(column.Name))
	}
	s.UpdateFieldEncodings(dynamicEncodings)
}

func (s *schemaRegistry) UpdateFieldEncodings(encodings map[FieldEncodingKey]EncodedFieldName) {
	s.fieldEncodingsLock.Lock()
	defer s.fieldEncodingsLock.Unlock()
	for key, value := range encodings {
		s.fieldEncodings[key] = EncodedFieldName(value)
	}
}

func (s *schemaRegistry) GetFieldEncodings() map[FieldEncodingKey]EncodedFieldName {
	s.fieldEncodingsLock.RLock()
	defer s.fieldEncodingsLock.RUnlock()
	fieldEncodings := make(map[FieldEncodingKey]EncodedFieldName)
	for key, value := range s.fieldEncodings {
		fieldEncodings[key] = EncodedFieldName(value)

	}
	return fieldEncodings
}

func NewSchemaRegistry(tableProvider TableProvider, configuration *config.QuesmaConfiguration, dataSourceTypeAdapter typeAdapter) Registry {
	return &schemaRegistry{
		indexConfiguration:      &configuration.IndexConfig,
		dataSourceTableProvider: tableProvider,
		dataSourceTypeAdapter:   dataSourceTypeAdapter,
		dynamicConfiguration:    make(map[string]Table),
		fieldEncodings:          make(map[FieldEncodingKey]EncodedFieldName),
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
			// encode internalPropertyName according to defined rules
			internalPropertyName := util.FieldToColumnEncoder(fieldName.AsString())
			fields[FieldName(fieldName)] = Field{PropertyName: FieldName(fieldName), InternalPropertyName: FieldName(internalPropertyName), Type: resolvedType}
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

func (s *schemaRegistry) populateSchemaFromTableDefinition(definitions map[string]Table, indexName string, fields map[FieldName]Field, internalToPublicFieldsEncodings map[EncodedFieldName]string) (existsInDataSource bool) {

	tableDefinition, found := definitions[indexName]
	if found {
		logger.Debug().Msgf("loading schema for table %s", indexName)

		for _, column := range tableDefinition.Columns {

			var propertyName FieldName
			if internalField, ok := internalToPublicFieldsEncodings[EncodedFieldName(column.Name)]; ok {
				propertyName = FieldName(internalField)
				// if field encodings are not coming from ingest e.g. encodings map
				// is empty, read them from persistent storage, e.g. column comment
			} else if len(column.Comment) > 0 {
				propertyName = FieldName(column.Name)

				metadata, err := comment_metadata.UnmarshallCommentMetadata(column.Comment)
				if err != nil {
					logger.Warn().Msgf("error unmarshalling column '%s' (table: %s)  comment metadata: %s %v", indexName, column.Name, column.Comment, err)
				} else {
					if metadata != nil {
						if fieldName, ok := metadata.Values[comment_metadata.ElasticFieldName]; ok {
							propertyName = FieldName(fieldName)
						}
					}
				}

			} else {
				// if field encoding is not found, use the column name as the property name
				propertyName = FieldName(column.Name)
			}
			if existing, exists := fields[propertyName]; !exists {
				if quesmaType, resolved := s.dataSourceTypeAdapter.Convert(column.Type); resolved {
					fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), InternalPropertyType: column.Type, Type: quesmaType}
				} else {
					logger.Debug().Msgf("type %s not supported, falling back to keyword", column.Type)
					fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), InternalPropertyType: column.Type, Type: QuesmaTypeKeyword}
				}
			} else {
				fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), InternalPropertyType: column.Type, Type: existing.Type, Origin: existing.Origin}
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

func (s *schemaRegistry) populateFieldsOrigins(indexName string, fields map[FieldName]Field) {
	s.fieldOriginsLock.RLock()
	if fieldOrigins, ok := s.fieldOrigins[IndexName(indexName)]; ok {
		for fieldName, field := range fields {
			if origin, ok := fieldOrigins[field.InternalPropertyName]; ok {
				field.Origin = origin
				fields[fieldName] = field
			}
		}
	}
	s.fieldOriginsLock.RUnlock()
}

func (s *schemaRegistry) UpdateFieldsOrigins(name IndexName, fields map[FieldName]FieldSource) {
	s.fieldOriginsLock.Lock()
	defer s.fieldOriginsLock.Unlock()
	if s.fieldOrigins == nil {
		s.fieldOrigins = make(map[IndexName]map[FieldName]FieldSource)
	}
	s.fieldOrigins[name] = fields
}
