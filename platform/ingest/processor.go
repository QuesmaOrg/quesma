// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	"github.com/QuesmaOrg/quesma/platform/end_user_errors"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/recovery"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/stats"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/QuesmaOrg/quesma/platform/v2/core/diag"
	"github.com/goccy/go-json"
	"slices"
	"sort"
	"strings"
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

type Processor struct {
	ctx             context.Context
	cancel          context.CancelFunc
	chDb            quesma_api.BackendConnector
	tableDiscovery  database_common.TableDiscovery
	cfg             *config.QuesmaConfiguration
	phoneHomeClient diag.PhoneHomeClient
	schemaRegistry  schema.Registry
	tableResolver   table_resolver.TableResolver

	indexNameRewriter IndexNameRewriter

	errorLogCounter atomic.Int64
	lowerers        map[quesma_api.BackendConnectorType]Lowerer
	lowerer         *SqlLowerer
}

type (
	IngestProcessor Processor
	TableMap        = util.SyncMap[string, *database_common.Table]
	SchemaMap       = map[string]interface{} // TODO remove
	Attribute       struct {
		KeysArrayName   string
		ValuesArrayName string
		TypesArrayName  string
		MapValueName    string
		MapMetadataName string
		Type            database_common.BaseType
	}
)

func (ip *IngestProcessor) RegisterLowerer(lowerer Lowerer, connectorType quesma_api.BackendConnectorType) {
	ip.lowerers[connectorType] = lowerer
}

func NewTableMap() *TableMap {
	return util.NewSyncMap[string, *database_common.Table]()
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
		defer recovery.LogPanic()
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

func addOurFieldsToCreateTableStatement(
	stmt CreateTableStatement,
	config *database_common.ChTableConfig,
	table *database_common.Table,
) CreateTableStatement {
	// Early exit if no attributes and timestamp is already handled
	if len(config.Attributes) == 0 {
		_, ok := table.Cols[timestampFieldName]
		if !config.HasTimestamp || ok {
			return stmt
		}
	}
	// Handle attribute columns
	for _, attr := range config.Attributes {
		// Add metadata Map
		if _, ok := table.Cols[attr.MapMetadataName]; !ok {
			stmt.Columns = append([]ColumnStatement{
				{
					ColumnName:         attr.MapMetadataName,
					ColumnType:         "Map(String,String)",
					AdditionalMetadata: "",
				},
			}, stmt.Columns...)
			table.Cols[attr.MapMetadataName] = &database_common.Column{
				Name: attr.MapMetadataName,
				Type: database_common.CompoundType{
					Name:     "Map",
					BaseType: database_common.NewBaseType("String, String"),
				},
			}
		}
		// Add value Map
		if _, ok := table.Cols[attr.MapValueName]; !ok {
			stmt.Columns = append([]ColumnStatement{
				{
					ColumnName:         attr.MapValueName,
					ColumnType:         "Map(String,String)",
					AdditionalMetadata: "",
				},
			}, stmt.Columns...)
			table.Cols[attr.MapValueName] = &database_common.Column{
				Name: attr.MapValueName,
				Type: database_common.CompoundType{
					Name:     "Map",
					BaseType: database_common.NewBaseType("String, String"),
				},
			}
		}

	}

	// Handle timestamp column
	if config.HasTimestamp {
		if _, ok := table.Cols[timestampFieldName]; !ok {
			defaultStr := ""
			if config.TimestampDefaultsNow {
				defaultStr = "DEFAULT now64()"
			}

			// Add to statement
			stmt.Columns = append([]ColumnStatement{
				{
					ColumnName:         timestampFieldName,
					ColumnType:         "DateTime64(3)",
					Comment:            "",
					AdditionalMetadata: defaultStr,
				},
			}, stmt.Columns...)

			// Update table metadata
			table.Cols[timestampFieldName] = &database_common.Column{
				Name: timestampFieldName,
				Type: database_common.NewBaseType("DateTime64"),
			}
		}
	}

	return stmt
}

func (ip *IngestProcessor) Count(ctx context.Context, table string) (int64, error) {
	var count int64
	err := ip.chDb.QueryRow(ctx, "SELECT count(*) FROM ?", table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	return count, nil
}

func (ip *IngestProcessor) createTableObject(tableName string, columnsFromJson []CreateTableEntry, columnsFromSchema map[schema.FieldName]CreateTableEntry, tableConfig *database_common.ChTableConfig) *database_common.Table {
	resolveType := func(name, colType string) database_common.Type {
		if strings.Contains(colType, " DEFAULT") {
			// Remove DEFAULT clause from the type
			colType = strings.Split(colType, " DEFAULT")[0]
		}
		resCol := database_common.ResolveColumn(name, colType, database_common.ClickHouseInstance)
		return resCol.Type
	}

	tableColumns := make(map[string]*database_common.Column)
	for _, c := range columnsFromJson {
		tableColumns[c.ClickHouseColumnName] = &database_common.Column{
			Name: c.ClickHouseColumnName,
			Type: resolveType(c.ClickHouseColumnName, c.ClickHouseType),
		}
	}
	for _, c := range columnsFromSchema {
		if _, exists := tableColumns[c.ClickHouseColumnName]; !exists {
			tableColumns[c.ClickHouseColumnName] = &database_common.Column{
				Name: c.ClickHouseColumnName,
				Type: resolveType(c.ClickHouseColumnName, c.ClickHouseType),
			}
		}
	}

	table := database_common.Table{
		Name:   tableName,
		Cols:   tableColumns,
		Config: tableConfig,
	}

	return &table
}

func (ip *IngestProcessor) createTableObjectAndAttributes(ctx context.Context, tableName string, columnsFromJson []CreateTableEntry, columnsFromSchema map[schema.FieldName]CreateTableEntry, tableConfig *database_common.ChTableConfig, tableDefinitionChangeOnly bool) (*database_common.Table, error) {
	table := ip.createTableObject(tableName, columnsFromJson, columnsFromSchema, tableConfig)

	// This is a HACK.
	// CreateQueryParser assumes that the table name is in the form of "database.table"
	// in this case we don't have a database name, so we need to add it
	if tableDefinitionChangeOnly {
		table.DatabaseName = ""
		table.Comment = "Definition only. This is not a real table."
		table.VirtualTable = true
	}

	// if exists only then createTable
	noSuchTable := ip.AddTableIfDoesntExist(table)
	if !noSuchTable {
		return nil, fmt.Errorf("table %s already exists", table.Name)
	}

	return table, nil
}

func findSchemaPointer(schemaRegistry schema.Registry, tableName string) *schema.Schema {
	if foundSchema, found := schemaRegistry.FindSchema(schema.IndexName(tableName)); found {
		return &foundSchema
	}
	return nil
}

func Indexes(m SchemaMap) string {
	var result strings.Builder
	for col := range m {
		index := database_common.GetIndexStatement(col)
		if index != "" {
			result.WriteString(",\n")
			result.WriteString(util.Indent(1))
			result.WriteString(index.Statement())
		}
	}
	result.WriteString(",\n")
	return result.String()
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
		newAttrsMap[database_common.DeprecatedAttributesKeyColumn] = append(newAttrsMap[database_common.DeprecatedAttributesKeyColumn], k)
		newAttrsMap[database_common.DeprecatedAttributesValueColumn] = append(newAttrsMap[database_common.DeprecatedAttributesValueColumn], v)

		valueType, err := database_common.NewType(v, k)
		if err != nil {
			newAttrsMap[database_common.DeprecatedAttributesValueType] = append(newAttrsMap[database_common.DeprecatedAttributesValueType], database_common.UndefinedType)
		} else {
			newAttrsMap[database_common.DeprecatedAttributesValueType] = append(newAttrsMap[database_common.DeprecatedAttributesValueType], valueType.String())
		}
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

// This struct contains the information about the columns that aren't part of the schema
// and will go into attributes map
type NonSchemaField struct {
	Key   string
	Value string
	Type  string // inferred from incoming json
}

func convertNonSchemaFieldsToMap(nonSchemaFields []NonSchemaField) map[string]any {
	valuesMap := make(map[string]string)
	typesMap := make(map[string]string)

	for _, f := range nonSchemaFields {
		if f.Value != "" {
			valuesMap[f.Key] = f.Value
		}
		if f.Type != "" {
			typesMap[f.Key] = f.Type
		}
	}

	result := make(map[string]any)

	if len(valuesMap) > 0 {
		result[database_common.AttributesValuesColumn] = valuesMap
	}
	if len(typesMap) > 0 {
		result[database_common.AttributesMetadataColumn] = typesMap
	}

	return result
}

func generateNonSchemaFields(attrsMap map[string][]interface{}) ([]NonSchemaField, error) {
	var nonSchemaFields []NonSchemaField
	if len(attrsMap) <= 0 {
		return nonSchemaFields, nil
	}
	attrKeys := getAttributesByArrayName(database_common.DeprecatedAttributesKeyColumn, attrsMap)
	attrValues := getAttributesByArrayName(database_common.DeprecatedAttributesValueColumn, attrsMap)
	attrTypes := getAttributesByArrayName(database_common.DeprecatedAttributesValueType, attrsMap)

	attributesColumns := []string{database_common.AttributesValuesColumn, database_common.AttributesMetadataColumn}

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
func (ip *SqlLowerer) shouldAlterColumns(table *database_common.Table, attrsMap map[string][]interface{}) (bool, []int) {
	attrKeys := getAttributesByArrayName(database_common.DeprecatedAttributesKeyColumn, attrsMap)
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

func generateInsertJson(nonSchemaFields []NonSchemaField, onlySchemaFields types.JSON) (string, error) {
	result := convertNonSchemaFieldsToMap(nonSchemaFields)

	for k, v := range onlySchemaFields {
		result[k] = v
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func generateSqlStatements(createTableCmd CreateTableStatement, alterStatments []AlterStatement, insertStatement InsertStatement) []string {
	var statements []string
	if createTableCmd.Name != "" {
		statements = append(statements, createTableCmd.ToSQL())
	}
	for _, alter := range alterStatments {
		statements = append(statements, alter.ToSql())
	}
	statements = append(statements, insertStatement.ToSQL())
	return statements
}

func populateFieldEncodings(jsonData []types.JSON, tableName string) map[schema.FieldEncodingKey]schema.EncodedFieldName {
	encodings := make(map[schema.FieldEncodingKey]schema.EncodedFieldName)
	for _, jsonValue := range jsonData {
		flattenJson := util.FlattenMap(jsonValue, ".")
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
	jsonData []types.JSON, transformer IngestTransformer,
	tableFormatter TableColumNameFormatter, tableDefinitionChangeOnly bool) ([]string, error) {
	// this is pre ingest transformer
	// here we transform the data before it's structure evaluation and insertion
	//
	preIngestTransformer := &util.RewriteArrayOfObject{}
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
		transformFieldName(jsonValue, util.FieldToColumnEncoder, util.FieldPartToColumnEncoder)
	}

	var transformedJsons []types.JSON
	for _, jsonValue := range jsonData {
		transformedJson, err := transformer.Transform(jsonValue)
		if err != nil {
			return nil, fmt.Errorf("error while transforming json: %v", err)
		}
		transformedJsons = append(transformedJsons, transformedJson)
	}

	table := ip.FindTable(tableName)
	var tableConfig *database_common.ChTableConfig
	var createTableCmd CreateTableStatement
	if table == nil {
		tableConfig = NewOnlySchemaFieldsCHConfig(ip.cfg.ClusterName)
		if indexConfig, ok := ip.cfg.IndexConfig[tableName]; ok {
			tableConfig.PartitionStrategy = indexConfig.PartitioningStrategy
		} else if strategy := ip.cfg.DefaultPartitioningStrategy; strategy != "" {
			tableConfig.PartitionStrategy = strategy
		}
		columnsFromJson := JsonToColumns(transformedJsons[0], tableConfig)

		fieldOrigins := make(map[schema.FieldName]schema.FieldSource)

		for _, column := range columnsFromJson {
			fieldOrigins[schema.FieldName(column.ClickHouseColumnName)] = schema.FieldSourceIngest
		}

		ip.schemaRegistry.UpdateFieldsOrigins(schema.IndexName(tableName), fieldOrigins)

		// This comes externally from (configuration), therefore we need to convert that separately
		columnsFromSchema := SchemaToColumns(findSchemaPointer(ip.schemaRegistry, tableName), tableFormatter, tableName, ip.schemaRegistry.GetFieldEncodings())
		resultColumns := columnsToProperties(columnsFromJson, columnsFromSchema, ip.schemaRegistry.GetFieldEncodings(), tableName)
		createTableCmd = BuildCreateTable(tableName, resultColumns, Indexes(transformedJsons[0]), tableConfig)
		var err error
		table, err = ip.createTableObjectAndAttributes(ctx, tableName, columnsFromJson, columnsFromSchema, tableConfig, tableDefinitionChangeOnly)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error createTableObjectAndAttributes, can't create table: %v", err)
			return nil, err
		} else {
			// Likely we want to remove below line
			createTableCmd = addOurFieldsToCreateTableStatement(createTableCmd, tableConfig, table)
		}
	}

	if table == nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	var validatedJsons []types.JSON
	var invalidJsons []types.JSON
	validatedJsons, invalidJsons, err := ip.preprocessJsons(ctx, table.Name, transformedJsons)
	if err != nil {
		return nil, fmt.Errorf("error preprocessJsons: %v", err)
	}
	ddlLowerer, ok := ip.lowerers[ip.chDb.GetId()]
	if !ok {
		return nil, fmt.Errorf("no lowerer registered for connector type %s", quesma_api.GetBackendConnectorNameFromType(ip.chDb.GetId()))
	}
	return ddlLowerer.LowerToDDL(validatedJsons, table, invalidJsons, encodings, createTableCmd)
}

func (lm *IngestProcessor) Ingest(ctx context.Context, indexName string, jsonData []types.JSON) error {

	err := elasticsearch.IsValidIndexName(indexName)
	if err != nil {
		return err
	}

	nameFormatter := DefaultColumnNameFormatter()
	transformer := IngestTransformerFor(indexName, lm.cfg)
	return lm.ProcessInsertQuery(ctx, indexName, jsonData, transformer, nameFormatter)
}

func (lm *IngestProcessor) ProcessInsertQuery(ctx context.Context, tableName string,
	jsonData []types.JSON, transformer IngestTransformer,
	tableFormatter TableColumNameFormatter) error {

	decision := lm.tableResolver.Resolve(quesma_api.IngestPipeline, tableName)

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

		var clickhouseDecision *quesma_api.ConnectorDecisionClickhouse

		var ok bool
		if clickhouseDecision, ok = connectorDecision.(*quesma_api.ConnectorDecisionClickhouse); !ok {
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

			pipeline := IngestTransformerPipeline{}
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

func (ip *IngestProcessor) applyAsyncInsertOptimizer(tableName string, clickhouseSettings clickhouse.Settings) clickhouse.Settings {

	const asyncInsertOptimizerName = "async_insert"
	enableAsyncInsert := true // enabled by default
	var asyncInsertProps map[string]string

	if optimizer, ok := ip.cfg.DefaultIngestOptimizers[asyncInsertOptimizerName]; ok {
		enableAsyncInsert = !optimizer.Disabled
		asyncInsertProps = optimizer.Properties
	}

	idxCfg, ok := ip.cfg.IndexConfig[tableName]
	if ok {
		if optimizer, ok := idxCfg.Optimizers[asyncInsertOptimizerName]; ok {
			enableAsyncInsert = !optimizer.Disabled
			asyncInsertProps = optimizer.Properties
		}
	}

	if enableAsyncInsert {
		clickhouseSettings["async_insert"] = 1

		// some sane defaults
		clickhouseSettings["wait_for_async_insert"] = 1

		clickhouseSettings["async_insert_busy_timeout_ms"] = 100      // default is 1000ms
		clickhouseSettings["async_insert_max_data_size"] = 50_000_000 // default is 10MB
		clickhouseSettings["async_insert_max_query_number"] = 10000   // default is 450

		for k, v := range asyncInsertProps {
			clickhouseSettings[k] = v
		}
	}

	return clickhouseSettings
}

func (ip *IngestProcessor) processInsertQueryInternal(ctx context.Context, tableName string,
	jsonData []types.JSON, transformer IngestTransformer,
	tableFormatter TableColumNameFormatter, isVirtualTable bool) error {
	statements, err := ip.processInsertQuery(ctx, tableName, jsonData, transformer, tableFormatter, isVirtualTable)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("error processing insert query: %v", err)
		return err
	}

	var logVirtualTableDDL bool // maybe this should be a part of the config or sth

	for _, statement := range statements {
		if strings.HasPrefix(statement, "ALTER") || strings.HasPrefix(statement, "CREATE") {
			if isVirtualTable && logVirtualTableDDL {
				logger.InfoWithCtx(ctx).Msgf("VIRTUAL DDL EXECUTION: %s", statement)
			} else {
				logger.InfoWithCtx(ctx).Msgf("DDL EXECUTION: %s", statement)
			}
		}
	}

	if isVirtualTable {
		return nil
	}

	clickhouseSettings := clickhouse.Settings{
		"date_time_input_format": "best_effort",
	}

	clickhouseSettings = ip.applyAsyncInsertOptimizer(tableName, clickhouseSettings)

	// We expect to have date format set to `best_effort`
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouseSettings))

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
	span := ip.phoneHomeClient.ClickHouseInsertDuration().Begin()

	// We log every DDL query
	if ip.cfg.Logging.EnableSQLTracing {
		if strings.HasPrefix(query, "ALTER") || strings.HasPrefix(query, "CREATE") {
			logger.InfoWithCtx(ctx).Msgf("DDL query execution: %s", query)
		}
	}

	err := ip.chDb.Exec(ctx, query)
	span.End(err)
	return err
}

func (ip *IngestProcessor) executeStatements(ctx context.Context, queries []string) error {
	for _, q := range queries {

		err := ip.execute(ctx, q)
		if err != nil {
			if strings.Contains(q, "CREATE") { // always log table creation failures
				logger.Error().Err(err).Msgf("Error executing DDL: %s", q)
			}
			count := ip.errorLogCounter.Add(1)

			// Limit the number of error logs to avoid flooding the logs.

			// some hardcoded limits
			const maxErrorLogs = 50
			const fullQueryThreshold = 5
			const maxQueryLength = 100
			const summaryInterval = 1000

			// logging only first nth errors, it should be enough for troubleshooting
			if count < maxErrorLogs {
				// only first fullQueryThreshold errors will be logged with full query
				if count > fullQueryThreshold {
					if len(q) > maxQueryLength {
						q = q[:maxQueryLength] + "..."
					}
				}
				logger.ErrorWithCtx(ctx).Msgf("error executing ingest statement: %s, query: %s", err, q)
			} else {
				// log every summaryInterval-th error, just to keep track of errors
				if count%summaryInterval == 0 {
					logger.ErrorWithCtx(ctx).Msgf("got %d total errors executing ingest statements.  last error: %s, last query: %s", count, err, q)
				}
			}

			return err
		}
	}
	return nil
}

func (ip *IngestProcessor) preprocessJsons(ctx context.Context,
	tableName string, jsons []types.JSON) ([]types.JSON, []types.JSON, error) {
	var validatedJsons []types.JSON
	var invalidJsons []types.JSON
	for _, jsonValue := range jsons {
		// Validate the input JSON
		// against the schema
		inValidJson, err := ip.validateIngest(tableName, jsonValue)
		if err != nil {
			return nil, nil, fmt.Errorf("error validation: %v", err)
		}
		invalidJsons = append(invalidJsons, inValidJson)
		if ip.cfg != nil {
			stats.GlobalStatistics.UpdateNonSchemaValues(ip.cfg.IngestStatistics, tableName, inValidJson, NestedSeparator)
		}
		// Remove invalid fields from the input JSON
		jsonValue = subtractInputJson(jsonValue, inValidJson)
		validatedJsons = append(validatedJsons, jsonValue)
	}
	return validatedJsons, invalidJsons, nil
}

func (ip *IngestProcessor) FindTable(tableName string) (result *database_common.Table) {
	tableNamePattern := util.TableNamePatternRegexp(tableName)
	ip.tableDiscovery.TableDefinitions().
		Range(func(name string, table *database_common.Table) bool {
			if tableNamePattern.MatchString(name) {
				result = table
				return false
			}
			return true
		})

	return result
}

func storeVirtualTable(table *database_common.Table, virtualTableStorage persistence.JSONDatabase) error {

	now := time.Now()

	table.Comment = "Virtual table. Version: " + now.Format(time.RFC3339)

	var columnsToStore []string
	for _, col := range table.Cols {
		// We don't want to store attributes columns in the virtual table
		if col.Name == database_common.AttributesValuesColumn || col.Name == database_common.AttributesMetadataColumn {
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

	return virtualTableStorage.Put(table.Name, string(data))
}

// Returns if schema wasn't created (so it needs to be, and will be in a moment)
func (ip *IngestProcessor) AddTableIfDoesntExist(table *database_common.Table) bool {
	t := ip.FindTable(table.Name)
	if t == nil {
		table.ApplyIndexConfig(ip.cfg)

		if table.VirtualTable {
			err := storeVirtualTable(table, ip.lowerer.virtualTableStorage)
			if err != nil {
				logger.Error().Msgf("error storing virtual table: %v", err)
			}
		}
		ip.tableDiscovery.AddTable(table.Name, table)
		return true
	}
	return false
}

func (ip *IngestProcessor) GetSchemaRegistry() schema.Registry {
	return ip.schemaRegistry
}

func (ip *IngestProcessor) GetTableResolver() table_resolver.TableResolver {
	return ip.tableResolver
}

func (ip *IngestProcessor) Ping() error {
	return ip.chDb.Ping()
}

func (ip *IngestProcessor) GetIndexNameRewriter() IndexNameRewriter {
	return ip.indexNameRewriter
}

func NewIngestProcessor(cfg *config.QuesmaConfiguration, chDb quesma_api.BackendConnector, phoneHomeClient diag.PhoneHomeClient, loader database_common.TableDiscovery, schemaRegistry schema.Registry, lowerer *SqlLowerer, tableResolver table_resolver.TableResolver) *IngestProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	indexRewriter := NewIndexNameRewriter(cfg)
	return &IngestProcessor{ctx: ctx, cancel: cancel, chDb: chDb,
		tableDiscovery: loader, cfg: cfg, phoneHomeClient: phoneHomeClient,
		schemaRegistry: schemaRegistry, lowerers: make(map[quesma_api.BackendConnectorType]Lowerer),
		lowerer: lowerer, tableResolver: tableResolver, indexNameRewriter: indexRewriter}
}

func NewOnlySchemaFieldsCHConfig(clusterName string) *database_common.ChTableConfig {
	return &database_common.ChTableConfig{
		HasTimestamp:                          true,
		TimestampDefaultsNow:                  true,
		Engine:                                "MergeTree",
		OrderBy:                               "(" + `"@timestamp"` + ")",
		ClusterName:                           clusterName,
		PrimaryKey:                            "",
		Ttl:                                   "",
		Attributes:                            []database_common.Attribute{database_common.NewDefaultStringAttribute()},
		CastUnsupportedAttrValueTypesToString: false,
		PreferCastingToOthers:                 false,
	}
}

// NewDefaultCHConfig is used only in tests
func NewDefaultCHConfig() *database_common.ChTableConfig {
	return &database_common.ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(" + `"@timestamp"` + ")",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []database_common.Attribute{
			database_common.NewDefaultInt64Attribute(),
			database_common.NewDefaultFloat64Attribute(),
			database_common.NewDefaultBoolAttribute(),
			database_common.NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}

// NewChTableConfigNoAttrs is used only in tests
func NewChTableConfigNoAttrs() *database_common.ChTableConfig {
	return &database_common.ChTableConfig{
		HasTimestamp:                          false,
		TimestampDefaultsNow:                  false,
		Engine:                                "MergeTree",
		OrderBy:                               "(" + `"@timestamp"` + ")",
		Attributes:                            []database_common.Attribute{},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}

// NewChTableConfigFourAttrs is used only in tests
func NewChTableConfigFourAttrs() *database_common.ChTableConfig {
	return &database_common.ChTableConfig{
		HasTimestamp:         false,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(" + "`@timestamp`" + ")",
		Attributes: []database_common.Attribute{
			database_common.NewDefaultInt64Attribute(),
			database_common.NewDefaultFloat64Attribute(),
			database_common.NewDefaultBoolAttribute(),
			database_common.NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}
