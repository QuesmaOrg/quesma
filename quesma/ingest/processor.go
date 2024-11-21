// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	chLib "quesma/clickhouse"
	"quesma/comment_metadata"
	"quesma/common_table"
	"quesma/end_user_errors"
	"quesma/index"
	"quesma/jsonprocessor"
	"quesma/logger"
	"quesma/model"
	"quesma/persistence"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/stats"
	"quesma/table_resolver"
	"quesma/telemetry"
	"quesma/util"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	timestampFieldName = "@timestamp" // it's always DateTime64 for now, don't want to waste time changing that, we don't seem to use that anyway
	// Above this number of columns we will use heuristic
	// to decide if we should add new columns
	alwaysAddColumnLimit  = 100
	alterColumnUpperLimit = 1000
	fieldFrequency        = 10
)

type (
	IngestFieldBucketKey struct {
		indexName string
		field     string
	}
	IngestFieldStatistics map[IngestFieldBucketKey]int64
)

type Ingester interface {
	Ingest(ctx context.Context, tableName string, jsonData []types.JSON) error
}

type (
	IngestProcessor struct {
		ctx                       context.Context
		cancel                    context.CancelFunc
		chDb                      *sql.DB
		tableDiscovery            chLib.TableDiscovery
		cfg                       *config.QuesmaConfiguration
		phoneHomeAgent            telemetry.PhoneHomeAgent
		schemaRegistry            schema.Registry
		ingestCounter             int64
		ingestFieldStatistics     IngestFieldStatistics
		ingestFieldStatisticsLock sync.Mutex
		virtualTableStorage       persistence.JSONDatabase
		tableResolver             table_resolver.TableResolver
	}
	TableMap  = util.SyncMap[string, *chLib.Table]
	SchemaMap = map[string]interface{} // TODO remove
	Attribute struct {
		KeysArrayName   string
		ValuesArrayName string
		TypesArrayName  string
		MapValueName    string
		MapMetadataName string
		Type            chLib.BaseType
	}
)

func NewTableMap() *TableMap {
	return util.NewSyncMap[string, *chLib.Table]()
}

func (ip *IngestProcessor) Start() {
	if err := ip.chDb.Ping(); err != nil {
		endUserError := end_user_errors.GuessClickhouseErrorType(err)
		logger.ErrorWithCtxAndReason(ip.ctx, endUserError.Reason()).Msgf("could not connect to clickhouse. error: %v", endUserError)
	}

	ip.tableDiscovery.ReloadTableDefinitions()

	logger.Info().Msgf("schemas loaded: %s", ip.tableDiscovery.TableDefinitions().Keys())
	const reloadInterval = 1 * time.Minute
	forceReloadCh := ip.tableDiscovery.ForceReloadCh()

	go func() {
		recovery.LogPanic()
		for {
			select {
			case <-ip.ctx.Done():
				logger.Debug().Msg("closing log manager")
				return
			case doneCh := <-forceReloadCh:
				// this prevents flood of reloads, after a long pause
				if time.Since(ip.tableDiscovery.LastReloadTime()) > reloadInterval {
					ip.tableDiscovery.ReloadTableDefinitions()
				}
				doneCh <- struct{}{}
			case <-time.After(reloadInterval):
				// only reload if we actually use Quesma, make it double time to prevent edge case
				// otherwise it prevent ClickHouse Cloud from idle pausing
				if time.Since(ip.tableDiscovery.LastAccessTime()) < reloadInterval*2 {
					ip.tableDiscovery.ReloadTableDefinitions()
				}
			}
		}
	}()
}

func (ip *IngestProcessor) Stop() {
	ip.cancel()
}

func (ip *IngestProcessor) Close() {
	_ = ip.chDb.Close()
}

// updates also Table TODO stop updating table here, find a better solution
func addOurFieldsToCreateTableQuery(q string, config *chLib.ChTableConfig, table *chLib.Table) string {
	if len(config.Attributes) == 0 {
		_, ok := table.Cols[timestampFieldName]
		if !config.HasTimestamp || ok {
			return q
		}
	}

	othersStr, timestampStr, attributesStr := "", "", ""
	if config.HasTimestamp {
		_, ok := table.Cols[timestampFieldName]
		if !ok {
			defaultStr := ""
			if config.TimestampDefaultsNow {
				defaultStr = " DEFAULT now64()"
			}
			timestampStr = fmt.Sprintf("%s\"%s\" DateTime64(3)%s,\n", util.Indent(1), timestampFieldName, defaultStr)
			table.Cols[timestampFieldName] = &chLib.Column{Name: timestampFieldName, Type: chLib.NewBaseType("DateTime64")}
		}
	}
	if len(config.Attributes) > 0 {
		for _, a := range config.Attributes {
			_, ok := table.Cols[a.MapValueName]
			if !ok {
				attributesStr += fmt.Sprintf("%s\"%s\" Map(String,String),\n", util.Indent(1), a.MapValueName)
				table.Cols[a.MapValueName] = &chLib.Column{Name: a.MapValueName, Type: chLib.CompoundType{Name: "Map", BaseType: chLib.NewBaseType("String, String")}}
			}
			_, ok = table.Cols[a.MapMetadataName]
			if !ok {
				attributesStr += fmt.Sprintf("%s\"%s\" Map(String,String),\n", util.Indent(1), a.MapMetadataName)
				table.Cols[a.MapMetadataName] = &chLib.Column{Name: a.MapMetadataName, Type: chLib.CompoundType{Name: "Map", BaseType: chLib.NewBaseType("String, String")}}
			}
		}
	}

	i := strings.Index(q, "(")
	return q[:i+2] + othersStr + timestampStr + attributesStr + q[i+1:]
}

func (ip *IngestProcessor) Count(ctx context.Context, table string) (int64, error) {
	var count int64
	err := ip.chDb.QueryRowContext(ctx, "SELECT count(*) FROM ?", table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	return count, nil
}

func (ip *IngestProcessor) createTableObjectAndAttributes(ctx context.Context, query string, config *chLib.ChTableConfig, name string, tableDefinitionChangeOnly bool) (string, error) {
	table, err := chLib.NewTable(query, config)
	if err != nil {
		return "", err
	}

	// This is a HACK.
	// CreateQueryParser assumes that the table name is in the form of "database.table"
	// in this case we don't have a database name, so we need to add it
	if tableDefinitionChangeOnly {
		table.Name = name
		table.DatabaseName = ""
		table.Comment = "Definition only. This is not a real table."
		table.VirtualTable = true
	}

	// if exists only then createTable
	noSuchTable := ip.AddTableIfDoesntExist(table)
	if !noSuchTable {
		return "", fmt.Errorf("table %s already exists", table.Name)
	}

	return addOurFieldsToCreateTableQuery(query, config, table), nil
}

func findSchemaPointer(schemaRegistry schema.Registry, tableName string) *schema.Schema {
	if foundSchema, found := schemaRegistry.FindSchema(schema.TableName(tableName)); found {
		return &foundSchema
	}
	return nil
}

func (ip *IngestProcessor) getIgnoredFields(tableName string) []config.FieldName {
	if indexConfig, found := ip.cfg.IndexConfig[tableName]; found && indexConfig.SchemaOverrides != nil {
		// FIXME: don't get ignored fields from schema config, but store
		// them in the schema registry - that way we don't have to manually replace '.' with '::'
		// in removeFieldsTransformer's Transform method
		return indexConfig.SchemaOverrides.IgnoredFields()
	}
	return nil
}

func Indexes(m SchemaMap) string {
	var result strings.Builder
	for col := range m {
		index := chLib.GetIndexStatement(col)
		if index != "" {
			result.WriteString(",\n")
			result.WriteString(util.Indent(1))
			result.WriteString(index.Statement())
		}
	}
	result.WriteString(",\n")
	return result.String()
}

func createTableQuery(name string, columns string, config *chLib.ChTableConfig) string {
	createTableCmd := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"
(

%s
)
%s
COMMENT 'created by Quesma'`,
		name, columns,
		config.CreateTablePostFieldsString())
	return createTableCmd
}

func columnsWithIndexes(columns string, indexes string) string {
	return columns + indexes

}

func deepCopyMapSliceInterface(original map[string][]interface{}) map[string][]interface{} {
	copiedMap := make(map[string][]interface{}, len(original))
	for key, value := range original {
		copiedSlice := make([]interface{}, len(value))
		copy(copiedSlice, value) // Copy the slice contents
		copiedMap[key] = copiedSlice
	}
	return copiedMap
}

// This function takes an attributesMap, creates a copy and updates it
// with the fields that are not valid according to the inferred schema
func addInvalidJsonFieldsToAttributes(attrsMap map[string][]interface{}, invalidJson types.JSON) map[string][]interface{} {
	newAttrsMap := deepCopyMapSliceInterface(attrsMap)
	for k, v := range invalidJson {
		newAttrsMap[chLib.DeprecatedAttributesKeyColumn] = append(newAttrsMap[chLib.DeprecatedAttributesKeyColumn], k)
		newAttrsMap[chLib.DeprecatedAttributesValueColumn] = append(newAttrsMap[chLib.DeprecatedAttributesValueColumn], v)
		newAttrsMap[chLib.DeprecatedAttributesValueType] = append(newAttrsMap[chLib.DeprecatedAttributesValueType], chLib.NewType(v).String())
	}
	return newAttrsMap
}

// This function takes an attributesMap and arrayName and returns
// the values of the array named arrayName from the attributesMap
func getAttributesByArrayName(arrayName string,
	attrsMap map[string][]interface{}) []string {
	var attributes []string
	for k, v := range attrsMap {
		if k == arrayName {
			for _, val := range v {
				attributes = append(attributes, util.Stringify(val))
			}
		}
	}
	return attributes
}

// This function generates ALTER TABLE commands for adding new columns
// to the table based on the attributesMap and the table name
// AttributesMap contains the attributes that are not part of the schema
// Function has side effects, it modifies the table.Cols map
// and removes the attributes that were promoted to columns
func (ip *IngestProcessor) generateNewColumns(
	attrsMap map[string][]interface{},
	table *chLib.Table,
	alteredAttributesIndexes []int,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName) []string {
	var alterCmd []string
	attrKeys := getAttributesByArrayName(chLib.DeprecatedAttributesKeyColumn, attrsMap)
	attrTypes := getAttributesByArrayName(chLib.DeprecatedAttributesValueType, attrsMap)
	var deleteIndexes []int

	reverseMap := reverseFieldEncoding(encodings, table.Name)

	// HACK Alert:
	// We must avoid altering the table.Cols map and reading at the same time.
	// This should be protected by a lock or a copy of the table should be used.
	//
	newColumns := make(map[string]*chLib.Column)
	for k, v := range table.Cols {
		newColumns[k] = v
	}

	for i := range alteredAttributesIndexes {

		columnType := ""
		modifiers := ""
		// Array and Map are not Nullable
		if strings.Contains(attrTypes[i], "Array") || strings.Contains(attrTypes[i], "Map") {
			columnType = attrTypes[i]
		} else {
			modifiers = "Nullable"
			columnType = fmt.Sprintf("Nullable(%s)", attrTypes[i])
		}

		propertyName := attrKeys[i]
		field, ok := reverseMap[schema.EncodedFieldName(attrKeys[i])]
		if ok {
			propertyName = field.FieldName
		}

		metadata := comment_metadata.NewCommentMetadata()
		metadata.Values[comment_metadata.ElasticFieldName] = propertyName
		comment := metadata.Marshall()

		alterTable := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table.Name, attrKeys[i], columnType)
		newColumns[attrKeys[i]] = &chLib.Column{Name: attrKeys[i], Type: chLib.NewBaseType(attrTypes[i]), Modifiers: modifiers, Comment: comment}
		alterCmd = append(alterCmd, alterTable)

		alterColumn := fmt.Sprintf("ALTER TABLE \"%s\" COMMENT COLUMN \"%s\" '%s'", table.Name, attrKeys[i], comment)
		alterCmd = append(alterCmd, alterColumn)

		deleteIndexes = append(deleteIndexes, i)
	}

	table.Cols = newColumns

	if table.VirtualTable {
		err := ip.storeVirtualTable(table)
		if err != nil {
			logger.Error().Msgf("error storing virtual table: %v", err)
		}
	}

	for i := len(deleteIndexes) - 1; i >= 0; i-- {
		attrsMap[chLib.DeprecatedAttributesKeyColumn] = append(attrsMap[chLib.DeprecatedAttributesKeyColumn][:deleteIndexes[i]], attrsMap[chLib.DeprecatedAttributesKeyColumn][deleteIndexes[i]+1:]...)
		attrsMap[chLib.DeprecatedAttributesValueType] = append(attrsMap[chLib.DeprecatedAttributesValueType][:deleteIndexes[i]], attrsMap[chLib.DeprecatedAttributesValueType][deleteIndexes[i]+1:]...)
		attrsMap[chLib.DeprecatedAttributesValueColumn] = append(attrsMap[chLib.DeprecatedAttributesValueColumn][:deleteIndexes[i]], attrsMap[chLib.DeprecatedAttributesValueColumn][deleteIndexes[i]+1:]...)
	}
	return alterCmd
}

// This struct contains the information about the columns that aren't part of the schema
// and will go into attributes map
type NonSchemaField struct {
	Key   string
	Value string
	Type  string // inferred from incoming json
}

func convertNonSchemaFieldsToString(nonSchemaFields []NonSchemaField) string {
	if len(nonSchemaFields) <= 0 {
		return ""
	}
	attributesColumns := []string{chLib.AttributesValuesColumn, chLib.AttributesMetadataColumn}
	var nonSchemaStr string
	for columnIndex, column := range attributesColumns {
		var value string
		if columnIndex > 0 {
			nonSchemaStr += ","
		}
		nonSchemaStr += "\"" + column + "\":{"
		for i := 0; i < len(nonSchemaFields); i++ {
			if columnIndex > 0 {
				value = nonSchemaFields[i].Type
			} else {
				value = nonSchemaFields[i].Value
			}
			if i > 0 {
				nonSchemaStr += ","
			}
			nonSchemaStr += fmt.Sprintf("\"%s\":\"%s\"", nonSchemaFields[i].Key, value)
		}
		nonSchemaStr = nonSchemaStr + "}"
	}
	return nonSchemaStr
}

func generateNonSchemaFields(attrsMap map[string][]interface{}) ([]NonSchemaField, error) {
	var nonSchemaFields []NonSchemaField
	if len(attrsMap) <= 0 {
		return nonSchemaFields, nil
	}
	attrKeys := getAttributesByArrayName(chLib.DeprecatedAttributesKeyColumn, attrsMap)
	attrValues := getAttributesByArrayName(chLib.DeprecatedAttributesValueColumn, attrsMap)
	attrTypes := getAttributesByArrayName(chLib.DeprecatedAttributesValueType, attrsMap)

	attributesColumns := []string{chLib.AttributesValuesColumn, chLib.AttributesMetadataColumn}

	for columnIndex := range attributesColumns {
		var value string
		for i := 0; i < len(attrKeys); i++ {
			if columnIndex > 0 {
				// We are versioning metadata fields
				// At the moment we store only types
				// but that might change in the future
				const metadataVersionPrefix = "v1"
				value = metadataVersionPrefix + ";" + attrTypes[i]
				if i > len(nonSchemaFields)-1 {
					nonSchemaFields = append(nonSchemaFields, NonSchemaField{Key: attrKeys[i], Value: "", Type: value})
				} else {
					nonSchemaFields[i].Type = value
				}
			} else {
				value = attrValues[i]
				if i > len(nonSchemaFields)-1 {
					nonSchemaFields = append(nonSchemaFields, NonSchemaField{Key: attrKeys[i], Value: value, Type: ""})
				} else {
					nonSchemaFields[i].Value = value
				}

			}
		}
	}
	return nonSchemaFields, nil
}

// This function implements heuristic for deciding if we should add new columns
func (ip *IngestProcessor) shouldAlterColumns(table *chLib.Table, attrsMap map[string][]interface{}) (bool, []int) {
	attrKeys := getAttributesByArrayName(chLib.DeprecatedAttributesKeyColumn, attrsMap)
	alterColumnIndexes := make([]int, 0)

	// this is special case for common table storage
	// we do always add columns for common table storage
	if table.Name == common_table.TableName {
		if len(table.Cols) > alterColumnUpperLimit {
			logger.Warn().Msgf("Common table has more than %d columns (alwaysAddColumnLimit)", alterColumnUpperLimit)
		}
	}

	if len(table.Cols) < alwaysAddColumnLimit || table.Name == common_table.TableName {
		// We promote all non-schema fields to columns
		// therefore we need to add all attrKeys indexes to alterColumnIndexes
		for i := 0; i < len(attrKeys); i++ {
			alterColumnIndexes = append(alterColumnIndexes, i)
		}
		return true, alterColumnIndexes
	}

	if len(table.Cols) > alterColumnUpperLimit {
		return false, nil
	}
	ip.ingestFieldStatisticsLock.Lock()
	if ip.ingestFieldStatistics == nil {
		ip.ingestFieldStatistics = make(IngestFieldStatistics)
	}
	ip.ingestFieldStatisticsLock.Unlock()
	for i := 0; i < len(attrKeys); i++ {
		ip.ingestFieldStatisticsLock.Lock()
		ip.ingestFieldStatistics[IngestFieldBucketKey{indexName: table.Name, field: attrKeys[i]}]++
		counter := atomic.LoadInt64(&ip.ingestCounter)
		fieldCounter := ip.ingestFieldStatistics[IngestFieldBucketKey{indexName: table.Name, field: attrKeys[i]}]
		// reset statistics every alwaysAddColumnLimit
		// for now alwaysAddColumnLimit is used in two contexts
		// for defining column limit and for resetting statistics
		if counter >= alwaysAddColumnLimit {
			atomic.StoreInt64(&ip.ingestCounter, 0)
			ip.ingestFieldStatistics = make(IngestFieldStatistics)
		}
		ip.ingestFieldStatisticsLock.Unlock()
		// if field is present more or equal fieldFrequency
		// during each alwaysAddColumnLimit iteration
		// promote it to column
		if fieldCounter >= fieldFrequency {
			alterColumnIndexes = append(alterColumnIndexes, i)
		}
	}
	if len(alterColumnIndexes) > 0 {
		return true, alterColumnIndexes
	}
	return false, nil
}

func (ip *IngestProcessor) GenerateIngestContent(table *chLib.Table,
	data types.JSON,
	inValidJson types.JSON,
	config *chLib.ChTableConfig,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName) ([]string, types.JSON, []NonSchemaField, error) {

	jsonAsBytesSlice, err := json.Marshal(data)

	if err != nil {
		return nil, nil, nil, err
	}

	// we find all non-schema fields
	jsonMap, err := types.ParseJSON(string(jsonAsBytesSlice))
	if err != nil {
		return nil, nil, nil, err
	}

	if len(config.Attributes) == 0 {
		return nil, jsonMap, nil, nil
	}

	schemaFieldsJson, err := json.Marshal(jsonMap)

	if err != nil {
		return nil, jsonMap, nil, err
	}

	mDiff := DifferenceMap(jsonMap, table) // TODO change to DifferenceMap(m, t)

	if len(mDiff) == 0 && string(schemaFieldsJson) == string(jsonAsBytesSlice) && len(inValidJson) == 0 { // no need to modify, just insert 'js'
		return nil, jsonMap, nil, nil
	}

	// check attributes precondition
	if len(config.Attributes) <= 0 {
		return nil, nil, nil, fmt.Errorf("no attributes config, but received non-schema fields: %s", mDiff)
	}
	attrsMap, _ := BuildAttrsMap(mDiff, config)

	// generateNewColumns is called on original attributes map
	// before adding invalid fields to it
	// otherwise it would contain invalid fields e.g. with wrong types
	// we only want to add fields that are not part of the schema e.g we don't
	// have columns for them
	var alterCmd []string
	atomic.AddInt64(&ip.ingestCounter, 1)
	if ok, alteredAttributesIndexes := ip.shouldAlterColumns(table, attrsMap); ok {
		alterCmd = ip.generateNewColumns(attrsMap, table, alteredAttributesIndexes, encodings)
	}
	// If there are some invalid fields, we need to add them to the attributes map
	// to not lose them and be able to store them later by
	// generating correct update query
	// addInvalidJsonFieldsToAttributes returns a new map with invalid fields added
	// this map is then used to generate non-schema fields string
	attrsMapWithInvalidFields := addInvalidJsonFieldsToAttributes(attrsMap, inValidJson)
	nonSchemaFields, err := generateNonSchemaFields(attrsMapWithInvalidFields)

	if err != nil {
		return nil, nil, nil, err
	}

	onlySchemaFields := RemoveNonSchemaFields(jsonMap, table)

	return alterCmd, onlySchemaFields, nonSchemaFields, nil
}

func generateInsertJson(nonSchemaFields []NonSchemaField, onlySchemaFields types.JSON) (string, error) {
	nonSchemaStr := convertNonSchemaFieldsToString(nonSchemaFields)
	schemaFieldsJson, err := json.Marshal(onlySchemaFields)
	if err != nil {
		return "", err
	}
	comma := ""
	if nonSchemaStr != "" && len(schemaFieldsJson) > 2 {
		comma = ","
	}
	return fmt.Sprintf("{%s%s%s", nonSchemaStr, comma, schemaFieldsJson[1:]), err
}

func generateSqlStatements(createTableCmd string, alterCmd []string, insert string) []string {
	var statements []string
	if createTableCmd != "" {
		statements = append(statements, createTableCmd)
	}
	statements = append(statements, alterCmd...)
	statements = append(statements, insert)
	return statements
}

func populateFieldEncodings(jsonData []types.JSON, tableName string) map[schema.FieldEncodingKey]schema.EncodedFieldName {
	encodings := make(map[schema.FieldEncodingKey]schema.EncodedFieldName)
	for _, jsonValue := range jsonData {
		flattenJson := jsonprocessor.FlattenMap(jsonValue, ".")
		for field := range flattenJson {
			encodedField := util.FieldToColumnEncoder(field)
			encodings[schema.FieldEncodingKey{TableName: tableName, FieldName: field}] =
				schema.EncodedFieldName(encodedField)
		}
	}
	return encodings
}

func (ip *IngestProcessor) processInsertQuery(ctx context.Context,
	tableName string,
	jsonData []types.JSON, transformer jsonprocessor.IngestTransformer,
	tableFormatter TableColumNameFormatter, tableDefinitionChangeOnly bool) ([]string, error) {
	// this is pre ingest transformer
	// here we transform the data before it's structure evaluation and insertion
	//
	preIngestTransformer := &jsonprocessor.RewriteArrayOfObject{}
	var processed []types.JSON
	for _, jsonValue := range jsonData {
		result, err := preIngestTransformer.Transform(jsonValue)
		if err != nil {
			return nil, fmt.Errorf("error while rewriting json: %v", err)
		}
		processed = append(processed, result)
	}
	jsonData = processed

	// we are doing two passes, e.g. calling transformFieldName twice
	// first time we populate encodings map
	// second time we do field encoding
	// This is because we need to know all field encodings before we start
	// transforming fields, otherwise we would mutate json and populating encodings
	// which would introduce side effects
	// This can be done in one pass, but it would be more complex
	// and requires some rewrite of json flattening
	encodings := populateFieldEncodings(jsonData, tableName)

	if ip.schemaRegistry != nil {
		ip.schemaRegistry.UpdateFieldEncodings(encodings)
	}
	// Do field encoding here, once for all jsons
	// This is in-place operation
	for _, jsonValue := range jsonData {
		transformFieldName(jsonValue, func(field string) string {
			return util.FieldToColumnEncoder(field)
		})
	}
	table := ip.FindTable(tableName)
	var tableConfig *chLib.ChTableConfig
	var createTableCmd string
	if table == nil {
		tableConfig = NewOnlySchemaFieldsCHConfig()
		ignoredFields := ip.getIgnoredFields(tableName)
		columnsFromJson := JsonToColumns("", jsonData[0], 1,
			tableConfig, tableFormatter, ignoredFields)

		fieldOrigins := make(map[schema.FieldName]schema.FieldSource)

		for _, column := range columnsFromJson {
			fieldOrigins[schema.FieldName(column.ClickHouseColumnName)] = schema.FieldSourceIngest
		}

		ip.schemaRegistry.UpdateFieldsOrigins(schema.TableName(tableName), fieldOrigins)

		// This comes externally from (configuration)
		// So we need to convert that separately
		columnsFromSchema := SchemaToColumns(findSchemaPointer(ip.schemaRegistry, tableName), tableFormatter, tableName, ip.schemaRegistry.GetFieldEncodings())
		columnsAsString := columnsWithIndexes(columnsToString(columnsFromJson, columnsFromSchema, ip.schemaRegistry.GetFieldEncodings(), tableName), Indexes(jsonData[0]))
		// TODO createTableCmd should contain information about field encodings
		// in column comments
		createTableCmd = createTableQuery(tableName, columnsAsString, tableConfig)
		var err error
		createTableCmd, err = ip.createTableObjectAndAttributes(ctx, createTableCmd, tableConfig, tableName, tableDefinitionChangeOnly)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error createTableObjectAndAttributes, can't create table: %v", err)
			return nil, err
		}
		// Set pointer to table after creating it
		table = ip.FindTable(tableName)
	} else if !table.Created {
		createTableCmd = table.CreateTableString()
	}
	if table == nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	tableConfig = table.Config
	var jsonsReadyForInsertion []string
	var alterCmd []string
	var preprocessedJsons []types.JSON
	var invalidJsons []types.JSON
	preprocessedJsons, invalidJsons, err := ip.preprocessJsons(ctx, table.Name, jsonData, transformer)
	if err != nil {
		return nil, fmt.Errorf("error preprocessJsons: %v", err)
	}
	for i, preprocessedJson := range preprocessedJsons {
		alter, onlySchemaFields, nonSchemaFields, err := ip.GenerateIngestContent(table, preprocessedJson,
			invalidJsons[i], tableConfig, encodings)

		if err != nil {
			return nil, fmt.Errorf("error BuildInsertJson, tablename: '%s' : %v", table.Name, err)
		}
		insertJson, err := generateInsertJson(nonSchemaFields, onlySchemaFields)
		if err != nil {
			return nil, fmt.Errorf("error generatateInsertJson, tablename: '%s' json: '%s': %v", table.Name, PrettyJson(insertJson), err)
		}
		alterCmd = append(alterCmd, alter...)
		if err != nil {
			return nil, fmt.Errorf("error BuildInsertJson, tablename: '%s' json: '%s': %v", table.Name, PrettyJson(insertJson), err)
		}
		jsonsReadyForInsertion = append(jsonsReadyForInsertion, insertJson)
	}

	insertValues := strings.Join(jsonsReadyForInsertion, ", ")
	insert := fmt.Sprintf("INSERT INTO \"%s\" FORMAT JSONEachRow %s", table.Name, insertValues)

	return generateSqlStatements(createTableCmd, alterCmd, insert), nil
}

func (lm *IngestProcessor) Ingest(ctx context.Context, tableName string, jsonData []types.JSON) error {

	nameFormatter := DefaultColumnNameFormatter()
	transformer := jsonprocessor.IngestTransformerFor(tableName, lm.cfg)
	return lm.ProcessInsertQuery(ctx, tableName, jsonData, transformer, nameFormatter)
}

func (lm *IngestProcessor) ProcessInsertQuery(ctx context.Context, tableName string,
	jsonData []types.JSON, transformer jsonprocessor.IngestTransformer,
	tableFormatter TableColumNameFormatter) error {

	decision := lm.tableResolver.Resolve(table_resolver.IngestPipeline, tableName)

	if decision.Err != nil {
		return decision.Err
	}

	if decision.IsEmpty { // TODO
		return fmt.Errorf("table %s not found", tableName)
	}

	if decision.IsClosed { // TODO
		return fmt.Errorf("table %s is closed", tableName)
	}

	for _, connectorDecision := range decision.UseConnectors {

		var clickhouseDecision *table_resolver.ConnectorDecisionClickhouse

		var ok bool
		if clickhouseDecision, ok = connectorDecision.(*table_resolver.ConnectorDecisionClickhouse); !ok {
			continue
		}

		if clickhouseDecision.IsCommonTable {

			// we have clone the data, because we want to process it twice
			var clonedJsonData []types.JSON
			for _, jsonValue := range jsonData {
				clonedJsonData = append(clonedJsonData, jsonValue.Clone())
			}

			err := lm.processInsertQueryInternal(ctx, tableName, clonedJsonData, transformer, tableFormatter, true)
			if err != nil {
				// we ignore an error here, because we want to process the data and don't lose it
				logger.ErrorWithCtx(ctx).Msgf("error processing insert query - virtual table schema update: %v", err)
			}

			pipeline := jsonprocessor.IngestTransformerPipeline{}
			pipeline = append(pipeline, &common_table.IngestAddIndexNameTransformer{IndexName: tableName})
			pipeline = append(pipeline, transformer)

			err = lm.processInsertQueryInternal(ctx, common_table.TableName, jsonData, pipeline, tableFormatter, false)
			if err != nil {
				return fmt.Errorf("error processing insert query to a common table: %w", err)
			}

		} else {
			err := lm.processInsertQueryInternal(ctx, clickhouseDecision.ClickhouseTableName, jsonData, transformer, tableFormatter, false)
			if err != nil {
				return fmt.Errorf("error processing insert query: %w", err)
			}
		}

	}
	return nil
}

func (ip *IngestProcessor) processInsertQueryInternal(ctx context.Context, tableName string,
	jsonData []types.JSON, transformer jsonprocessor.IngestTransformer,
	tableFormatter TableColumNameFormatter, isVirtualTable bool) error {
	statements, err := ip.processInsertQuery(ctx, tableName, jsonData, transformer, tableFormatter, isVirtualTable)
	if err != nil {
		return err
	}

	var logVirtualTableDDL bool // maybe this should be a part of the config or sth

	if isVirtualTable && logVirtualTableDDL {
		for _, statement := range statements {
			if strings.HasPrefix(statement, "ALTER") || strings.HasPrefix(statement, "CREATE") {
				logger.InfoWithCtx(ctx).Msgf("VIRTUAL DDL EXECUTION: %s", statement)
			}
		}
	}

	if isVirtualTable {
		return nil
	}

	// We expect to have date format set to `best_effort`
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"date_time_input_format": "best_effort",
	}))

	return ip.executeStatements(ctx, statements)
}

// This function removes fields that are part of anotherDoc from inputDoc
func subtractInputJson(inputDoc types.JSON, anotherDoc types.JSON) types.JSON {
	for key := range anotherDoc {
		delete(inputDoc, key)
	}
	return inputDoc
}

// This function executes query with context
// and creates span for it
func (ip *IngestProcessor) execute(ctx context.Context, query string) error {
	span := ip.phoneHomeAgent.ClickHouseInsertDuration().Begin()

	// We log every DDL query
	if ip.cfg.Logging.EnableSQLTracing {
		if strings.HasPrefix(query, "ALTER") || strings.HasPrefix(query, "CREATE") {
			logger.InfoWithCtx(ctx).Msgf("DDL query execution: %s", query)
		}
	}

	_, err := ip.chDb.ExecContext(ctx, query)
	span.End(err)
	return err
}

func (ip *IngestProcessor) executeStatements(ctx context.Context, queries []string) error {
	for _, q := range queries {

		err := ip.execute(ctx, q)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error executing query: %v", err)
			return err
		}
	}
	return nil
}

func (ip *IngestProcessor) preprocessJsons(ctx context.Context,
	tableName string, jsons []types.JSON, transformer jsonprocessor.IngestTransformer,
) ([]types.JSON, []types.JSON, error) {
	var preprocessedJsons []types.JSON
	var invalidJsons []types.JSON
	for _, jsonValue := range jsons {
		preprocessedJson, err := transformer.Transform(jsonValue)
		if err != nil {
			return nil, nil, fmt.Errorf("error IngestTransformer: %v", err)
		}
		// Validate the input JSON
		// against the schema
		inValidJson, err := ip.validateIngest(tableName, preprocessedJson)
		if err != nil {
			return nil, nil, fmt.Errorf("error validation: %v", err)
		}
		invalidJsons = append(invalidJsons, inValidJson)
		stats.GlobalStatistics.UpdateNonSchemaValues(ip.cfg, tableName,
			inValidJson, NestedSeparator)
		// Remove invalid fields from the input JSON
		preprocessedJson = subtractInputJson(preprocessedJson, inValidJson)
		preprocessedJsons = append(preprocessedJsons, preprocessedJson)
	}
	return preprocessedJsons, invalidJsons, nil
}

func (ip *IngestProcessor) FindTable(tableName string) (result *chLib.Table) {
	tableNamePattern := index.TableNamePatternRegexp(tableName)
	ip.tableDiscovery.TableDefinitions().
		Range(func(name string, table *chLib.Table) bool {
			if tableNamePattern.MatchString(name) {
				result = table
				return false
			}
			return true
		})

	return result
}

func (ip *IngestProcessor) storeVirtualTable(table *chLib.Table) error {

	now := time.Now()

	table.Comment = "Virtual table. Version: " + now.Format(time.RFC3339)

	var columnsToStore []string
	for _, col := range table.Cols {
		// We don't want to store attributes columns in the virtual table
		if col.Name == chLib.AttributesValuesColumn || col.Name == chLib.AttributesMetadataColumn {
			continue
		}
		columnsToStore = append(columnsToStore, col.Name)
	}

	// We always want to store timestamp in the virtual table
	// if it's not already there
	if !slices.Contains(columnsToStore, model.TimestampFieldName) {
		columnsToStore = append(columnsToStore, model.TimestampFieldName)
	}

	sort.Strings(columnsToStore)

	var columns []common_table.VirtualTableColumn

	for _, col := range columnsToStore {
		columns = append(columns, common_table.VirtualTableColumn{
			Name: col,
		})
	}

	vTable := &common_table.VirtualTable{
		Version:  common_table.VirtualTableStructVersion,
		StoredAt: now.Format(time.RFC3339),
		Columns:  columns,
	}

	data, err := json.MarshalIndent(vTable, "", "  ")
	if err != nil {
		return err
	}

	return ip.virtualTableStorage.Put(table.Name, string(data))
}

// Returns if schema wasn't created (so it needs to be, and will be in a moment)
func (ip *IngestProcessor) AddTableIfDoesntExist(table *chLib.Table) bool {
	t := ip.FindTable(table.Name)
	if t == nil {
		table.Created = true

		table.ApplyIndexConfig(ip.cfg)

		if table.VirtualTable {
			err := ip.storeVirtualTable(table)
			if err != nil {
				logger.Error().Msgf("error storing virtual table: %v", err)
			}
		}
		ip.tableDiscovery.TableDefinitions().Store(table.Name, table)
		return true
	}
	wasntCreated := !t.Created
	t.Created = true
	return wasntCreated
}

func (ip *IngestProcessor) Ping() error {
	return ip.chDb.Ping()
}

func NewIngestProcessor(cfg *config.QuesmaConfiguration, chDb *sql.DB, phoneHomeAgent telemetry.PhoneHomeAgent, loader chLib.TableDiscovery, schemaRegistry schema.Registry, virtualTableStorage persistence.JSONDatabase, tableResolver table_resolver.TableResolver) *IngestProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &IngestProcessor{ctx: ctx, cancel: cancel, chDb: chDb, tableDiscovery: loader, cfg: cfg, phoneHomeAgent: phoneHomeAgent, schemaRegistry: schemaRegistry, virtualTableStorage: virtualTableStorage, tableResolver: tableResolver}
}

func NewOnlySchemaFieldsCHConfig() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
		HasTimestamp:                          true,
		TimestampDefaultsNow:                  true,
		Engine:                                "MergeTree",
		OrderBy:                               "(" + `"@timestamp"` + ")",
		PartitionBy:                           "",
		PrimaryKey:                            "",
		Ttl:                                   "",
		Attributes:                            []chLib.Attribute{chLib.NewDefaultStringAttribute()},
		CastUnsupportedAttrValueTypesToString: false,
		PreferCastingToOthers:                 false,
	}
}

func NewDefaultCHConfig() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(" + `"@timestamp"` + ")",
		PartitionBy:          "",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []chLib.Attribute{
			chLib.NewDefaultInt64Attribute(),
			chLib.NewDefaultFloat64Attribute(),
			chLib.NewDefaultBoolAttribute(),
			chLib.NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}

func NewChTableConfigNoAttrs() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
		HasTimestamp:                          false,
		TimestampDefaultsNow:                  false,
		Engine:                                "MergeTree",
		OrderBy:                               "(" + `"@timestamp"` + ")",
		Attributes:                            []chLib.Attribute{},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}

func NewChTableConfigFourAttrs() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
		HasTimestamp:         false,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(" + "`@timestamp`" + ")",
		Attributes: []chLib.Attribute{
			chLib.NewDefaultInt64Attribute(),
			chLib.NewDefaultFloat64Attribute(),
			chLib.NewDefaultBoolAttribute(),
			chLib.NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}
