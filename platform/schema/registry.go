// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

import (
	"github.com/QuesmaOrg/quesma/platform/comment_metadata"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/recovery"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"log"
	"sync"
	"time"
)

// TODO we should rethink naming and types used in this package

type (
	Registry interface {
		Start()
		Stop()

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
		sync.RWMutex // this lock is used to protect all the fields below
		// locking is done in public methods only to avoid deadlocks

		// index configuration overrides always take precedence
		indexConfiguration      *map[string]config.IndexConfiguration
		defaultSchemaOverrides  *config.SchemaConfiguration
		dataSourceTableProvider TableProvider
		dataSourceTypeAdapter   typeAdapter
		dynamicConfiguration    map[string]Table
		fieldEncodings          map[FieldEncodingKey]EncodedFieldName
		fieldOrigins            map[IndexName]map[FieldName]FieldSource

		cachedSchemas map[IndexName]Schema

		doneCh chan struct{}
	}
	typeAdapter interface {
		Convert(string) (QuesmaType, bool)
	}
	TableProvider interface {
		RegisterTablesReloadListener(chan<- types.ReloadMessage)
		TableDefinitions() map[string]Table
		AutodiscoveryEnabled() bool
	}
	Table struct {
		Columns      map[string]Column
		DatabaseName string
	}
	Column struct {
		Name    string
		Type    string // FIXME: change to schema.Type
		Comment string
		Origin  FieldSource // TODO this field is just added to have way to forward information to the schema registry and should be considered as a technical debt
	}
)

func (s *schemaRegistry) getInternalToPublicFieldEncodings(tableName string) map[EncodedFieldName]string {
	fieldsEncodingsPerIndex := make(map[string]EncodedFieldName)

	for key, value := range s.fieldEncodings {
		if key.TableName == tableName {
			fieldsEncodingsPerIndex[key.FieldName] = EncodedFieldName(value)
		}
	}

	internalToPublicFieldsEncodings := make(map[EncodedFieldName]string)

	for key, value := range fieldsEncodingsPerIndex {
		internalToPublicFieldsEncodings[value] = key
	}

	return internalToPublicFieldsEncodings
}

func (s *schemaRegistry) invalidateCache() {
	s.cachedSchemas = nil
}

func (s *schemaRegistry) Start() {

	notificationChannel := make(chan types.ReloadMessage, 1)

	s.dataSourceTableProvider.RegisterTablesReloadListener(notificationChannel)

	protectedReload := func() {
		defer recovery.LogPanic()
		s.Lock()
		defer s.Unlock()

		s.invalidateCache()
	}

	go func() {
		// reload schemas every 5 minutes
		// table_discovery can be disabled, so we need to reload schemas periodically just in case
		ticker := time.NewTicker(5 * time.Minute)
		for {
			select {
			case <-notificationChannel:
				protectedReload()

			case <-ticker.C:
				protectedReload()

			case <-s.doneCh:
				return
			}
		}
	}()
}

func (s *schemaRegistry) Stop() {
	s.doneCh <- struct{}{}
}

func (s *schemaRegistry) loadOrGetSchemas() map[IndexName]Schema {

	if s.cachedSchemas == nil {
		schema, err := s.loadSchemas()
		if err != nil {
			logger.Error().Err(err).Msg("error loading schema")
			return make(map[IndexName]Schema)
		}
		s.cachedSchemas = schema
	}

	return s.cachedSchemas
}

func (s *schemaRegistry) loadSchemas() (map[IndexName]Schema, error) {
	definitions := s.dataSourceTableProvider.TableDefinitions()
	schemas := make(map[IndexName]Schema)

	if s.dataSourceTableProvider.AutodiscoveryEnabled() {
		for tableName, table := range definitions {
			fields := make(map[FieldName]Field)

			if tableName != common_table.TableName {
				_, hasConfig := (*s.indexConfiguration)[tableName]
				if !hasConfig && s.defaultSchemaOverrides != nil {
					log.Println("XXX apply default schema overrides for table", tableName)
					s.populateSchemaFromStaticConfiguration(s.defaultSchemaOverrides, fields)
				} else {
					log.Println("XXX dont apply default schema overrides for table", tableName)
					s.populateSchemaFromDynamicConfiguration(tableName, fields)
				}
			}

			internalToPublicFieldsEncodings := s.getInternalToPublicFieldEncodings(tableName)
			existsInDataSource := s.populateSchemaFromTableDefinition(definitions, tableName, fields, internalToPublicFieldsEncodings)
			schemas[IndexName(tableName)] = NewSchema(fields, existsInDataSource, table.DatabaseName)
		}
	}

	for indexName, indexConfiguration := range *s.indexConfiguration {
		fields := make(map[FieldName]Field)
		aliases := make(map[FieldName]FieldName)
		s.populateSchemaFromDynamicConfiguration(indexName, fields)
		s.populateSchemaFromStaticConfiguration(indexConfiguration.SchemaOverrides, fields)
		internalToPublicFieldsEncodings := s.getInternalToPublicFieldEncodings(indexName)
		tableName := indexConfiguration.TableName(indexName)
		existsInDataSource := s.populateSchemaFromTableDefinition(definitions, tableName, fields, internalToPublicFieldsEncodings)
		s.populateAliases(indexConfiguration, fields, aliases)
		s.removeIgnoredFields(indexConfiguration, fields, aliases)
		s.removeGeoPhysicalFields(fields)
		s.populateFieldsOrigins(indexName, fields)
		if tableDefinition, ok := definitions[indexName]; ok {
			schemas[IndexName(indexName)] = NewSchemaWithAliases(fields, aliases, existsInDataSource, tableDefinition.DatabaseName)
		} else {
			schemas[IndexName(indexName)] = NewSchemaWithAliases(fields, aliases, existsInDataSource, "")
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
	s.Lock()
	defer s.Unlock()

	return s.loadOrGetSchemas()
}

func (s *schemaRegistry) FindSchema(name IndexName) (Schema, bool) {
	s.Lock()
	defer s.Unlock()

	schemas := s.loadOrGetSchemas()

	schema, found := schemas[name]
	return schema, found

}

func (s *schemaRegistry) UpdateDynamicConfiguration(name IndexName, table Table) {
	s.Lock()
	defer s.Unlock()

	s.dynamicConfiguration[name.AsString()] = table
	dynamicEncodings := make(map[FieldEncodingKey]EncodedFieldName)
	for _, column := range table.Columns {
		// when table is created based on PUT `name/_mapping` we need to populate field encodings.
		// Otherwise, they will be populated only based on ingested data which might not contain all the fields
		dynamicEncodings[FieldEncodingKey{TableName: name.AsString(), FieldName: column.Name}] = EncodedFieldName(util.FieldToColumnEncoder(column.Name))
	}
	s.updateFieldEncodingsInternal(dynamicEncodings)
	s.invalidateCache()
}

func (s *schemaRegistry) updateFieldEncodingsInternal(encodings map[FieldEncodingKey]EncodedFieldName) {

	for key, value := range encodings {
		s.fieldEncodings[key] = EncodedFieldName(value)
	}
}

func (s *schemaRegistry) UpdateFieldEncodings(encodings map[FieldEncodingKey]EncodedFieldName) {
	s.Lock()
	defer s.Unlock()

	s.updateFieldEncodingsInternal(encodings)
	s.invalidateCache()
}

func (s *schemaRegistry) GetFieldEncodings() map[FieldEncodingKey]EncodedFieldName {
	s.RLock()
	defer s.RUnlock()

	fieldEncodings := make(map[FieldEncodingKey]EncodedFieldName)
	for key, value := range s.fieldEncodings {
		fieldEncodings[key] = EncodedFieldName(value)

	}
	return fieldEncodings
}

func NewSchemaRegistry(tableProvider TableProvider, configuration *config.QuesmaConfiguration, dataSourceTypeAdapter typeAdapter) Registry {
	res := &schemaRegistry{
		indexConfiguration:      &configuration.IndexConfig,
		defaultSchemaOverrides:  configuration.DefaultSchemaOverrides,
		dataSourceTableProvider: tableProvider,
		dataSourceTypeAdapter:   dataSourceTypeAdapter,
		dynamicConfiguration:    make(map[string]Table),
		cachedSchemas:           nil,
		fieldEncodings:          make(map[FieldEncodingKey]EncodedFieldName),
		doneCh:                  make(chan struct{}),
	}
	return res
}

func (s *schemaRegistry) populateSchemaFromStaticConfiguration(schemaOverrides *config.SchemaConfiguration, fields map[FieldName]Field) {
	if schemaOverrides == nil {
		return
	}
	for fieldName, field := range schemaOverrides.Fields {
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
					fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), InternalPropertyType: column.Type, Type: quesmaType, Origin: column.Origin}
				} else {
					logger.Debug().Msgf("type %s not supported, falling back to keyword", column.Type)
					fields[propertyName] = Field{PropertyName: propertyName, InternalPropertyName: FieldName(column.Name), InternalPropertyType: column.Type, Type: QuesmaTypeKeyword, Origin: column.Origin}
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

	if fieldOrigins, ok := s.fieldOrigins[IndexName(indexName)]; ok {
		for fieldName, field := range fields {
			if origin, ok := fieldOrigins[field.InternalPropertyName]; ok {
				field.Origin = origin
				fields[fieldName] = field
			}
		}
	}

}

func (s *schemaRegistry) UpdateFieldsOrigins(name IndexName, fields map[FieldName]FieldSource) {
	s.Lock()
	defer s.Unlock()

	if s.fieldOrigins == nil {
		s.fieldOrigins = make(map[IndexName]map[FieldName]FieldSource)
	}
	s.fieldOrigins[name] = fields
}
