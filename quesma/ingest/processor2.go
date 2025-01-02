// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/goccy/go-json"
	"net/http"
	"quesma/backend_connectors"
	chLib "quesma/clickhouse"
	"quesma/comment_metadata"
	"quesma/common_table"
	"quesma/end_user_errors"
	"quesma/jsonprocessor"
	"quesma/logger"
	"quesma/model"
	"quesma/persistence"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/stats"
	"quesma/telemetry"
	"quesma/util"
	quesma_api "quesma_v2/core"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type (
	//IngestProcessor2 is essentially an Ingest Processor we know and like but `chDb` is `quesma_api.BackendConnector` not `*sql.DB`
	IngestProcessor2 struct {
		ctx                       context.Context
		cancel                    context.CancelFunc
		chDb                      quesma_api.BackendConnector
		es                        backend_connectors.ElasticsearchBackendConnector
		tableDiscovery            chLib.TableDiscovery
		cfg                       *config.QuesmaConfiguration
		phoneHomeAgent            telemetry.PhoneHomeAgent
		schemaRegistry            schema.Registry
		ingestCounter             int64
		ingestFieldStatistics     IngestFieldStatistics
		ingestFieldStatisticsLock sync.Mutex
		virtualTableStorage       persistence.JSONDatabase
	}
)

func (ip *IngestProcessor2) Start() {
	if err := ip.Ping(); err != nil {
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

func (ip *IngestProcessor2) Stop() {
	ip.cancel()
}

func (ip *IngestProcessor2) Close() {
	_ = ip.chDb.Close()
}

//func (ip *IngestProcessor2) Count(ctx context.Context, table string) (int64, error) {
//	var count int64
//	err := ip.chDb.QueryRowContext(ctx, "SELECT count(*) FROM ?", table).Scan(&count)
//	if err != nil {
//		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
//	}
//	return count, nil
//}

func (ip *IngestProcessor2) SendToElasticsearch(req *http.Request) *http.Response {
	return ip.es.Send(req)
}

func (ip *IngestProcessor2) RequestToElasticsearch(ctx context.Context, method, endpoint string, body []byte, headers http.Header) (*http.Response, error) {
	return ip.es.RequestWithHeaders(ctx, method, endpoint, body, headers)
}

func (ip *IngestProcessor2) createTableObjectAndAttributes(ctx context.Context, query string, config *chLib.ChTableConfig, name string, tableDefinitionChangeOnly bool) (string, error) {
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

// This function generates ALTER TABLE commands for adding new columns
// to the table based on the attributesMap and the table name
// AttributesMap contains the attributes that are not part of the schema
// Function has side effects, it modifies the table.Cols map
// and removes the attributes that were promoted to columns
func (ip *IngestProcessor2) generateNewColumns(
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

// This function implements heuristic for deciding if we should add new columns
func (ip *IngestProcessor2) shouldAlterColumns(table *chLib.Table, attrsMap map[string][]interface{}) (bool, []int) {
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

func (ip *IngestProcessor2) GenerateIngestContent(table *chLib.Table,
	data types.JSON,
	inValidJson types.JSON,
	config *chLib.ChTableConfig,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName) ([]string, types.JSON, []NonSchemaField, error) {

	if len(config.Attributes) == 0 {
		return nil, data, nil, nil
	}

	mDiff := DifferenceMap(data, table) // TODO change to DifferenceMap(m, t)

	if len(mDiff) == 0 && len(inValidJson) == 0 { // no need to modify, just insert 'js'
		return nil, data, nil, nil
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

	onlySchemaFields := RemoveNonSchemaFields(data, table)

	return alterCmd, onlySchemaFields, nonSchemaFields, nil
}

func (ip *IngestProcessor2) processInsertQuery(ctx context.Context,
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

	var transformedJsons []types.JSON
	for _, jsonValue := range jsonData {
		transformedJson, err := transformer.Transform(jsonValue)
		if err != nil {
			return nil, fmt.Errorf("error while transforming json: %v", err)
		}
		transformedJsons = append(transformedJsons, transformedJson)
	}

	table := ip.FindTable(tableName)
	var tableConfig *chLib.ChTableConfig
	var createTableCmd string
	if table == nil {
		tableConfig = NewOnlySchemaFieldsCHConfig()
		columnsFromJson := JsonToColumns(transformedJsons[0], tableConfig)

		fieldOrigins := make(map[schema.FieldName]schema.FieldSource)

		for _, column := range columnsFromJson {
			fieldOrigins[schema.FieldName(column.ClickHouseColumnName)] = schema.FieldSourceIngest
		}

		ip.schemaRegistry.UpdateFieldsOrigins(schema.IndexName(tableName), fieldOrigins)

		// This comes externally from (configuration)
		// So we need to convert that separately
		columnsFromSchema := SchemaToColumns(findSchemaPointer(ip.schemaRegistry, tableName), tableFormatter, tableName, ip.schemaRegistry.GetFieldEncodings())
		columnsAsString := columnsWithIndexes(columnsToString(columnsFromJson, columnsFromSchema, ip.schemaRegistry.GetFieldEncodings(), tableName), Indexes(transformedJsons[0]))
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
	var validatedJsons []types.JSON
	var invalidJsons []types.JSON
	validatedJsons, invalidJsons, err := ip.preprocessJsons(ctx, table.Name, transformedJsons)
	if err != nil {
		return nil, fmt.Errorf("error preprocessJsons: %v", err)
	}
	for i, preprocessedJson := range validatedJsons {
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

func (lm *IngestProcessor2) Ingest(ctx context.Context, tableName string, jsonData []types.JSON) error {

	nameFormatter := DefaultColumnNameFormatter()
	transformer := jsonprocessor.IngestTransformerFor(tableName, lm.cfg) // here?
	return lm.ProcessInsertQuery(ctx, tableName, jsonData, transformer, nameFormatter)
}

func (lm *IngestProcessor2) useCommonTable(tableName string) bool {
	if tableConfig, ok := lm.cfg.IndexConfig[tableName]; ok {
		return tableConfig.UseCommonTable
	}
	return false
}

func (lm *IngestProcessor2) ProcessInsertQuery(ctx context.Context, tableName string,
	jsonData []types.JSON, transformer jsonprocessor.IngestTransformer,
	tableFormatter TableColumNameFormatter) error {

	if lm.useCommonTable(tableName) {
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
		err := lm.processInsertQueryInternal(ctx, tableName, jsonData, transformer, tableFormatter, false)
		if err != nil {
			return fmt.Errorf("error processing insert query: %w", err)
		}
	}

	return nil
}

func (ip *IngestProcessor2) applyAsyncInsertOptimizer(tableName string, clickhouseSettings clickhouse.Settings) clickhouse.Settings {

	const asyncInsertOptimizerName = "async_insert"
	enableAsyncInsert := false
	var asyncInsertProps map[string]string

	if optimizer, ok := ip.cfg.DefaultIngestOptimizers[asyncInsertOptimizerName]; ok {
		if !optimizer.Disabled {
			enableAsyncInsert = true
			asyncInsertProps = optimizer.Properties
		}
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
		clickhouseSettings["async_insert_busy_timeout_ms"] = 1000
		clickhouseSettings["async_insert_max_data_size"] = 1000000
		clickhouseSettings["async_insert_max_query_number"] = 10000

		for k, v := range asyncInsertProps {
			clickhouseSettings[k] = v
		}
	}

	return clickhouseSettings
}

func (ip *IngestProcessor2) processInsertQueryInternal(ctx context.Context, tableName string,
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

	clickhouseSettings := clickhouse.Settings{
		"date_time_input_format": "best_effort",
	}

	clickhouseSettings = ip.applyAsyncInsertOptimizer(tableName, clickhouseSettings)

	// We expect to have date format set to `best_effort`
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouseSettings))

	return ip.executeStatements(ctx, statements)
}

// This function executes query with context
// and creates span for it
func (ip *IngestProcessor2) execute(ctx context.Context, query string) error {
	//span := ip.phoneHomeAgent.ClickHouseInsertDuration().Begin()

	// We log every DDL query
	if ip.cfg.Logging.EnableSQLTracing {
		if strings.HasPrefix(query, "ALTER") || strings.HasPrefix(query, "CREATE") {
			logger.InfoWithCtx(ctx).Msgf("DDL query execution: %s", query)
		}
	}

	err := ip.chDb.Exec(ctx, query)
	//span.End(err)
	return err
}

func (ip *IngestProcessor2) executeStatements(ctx context.Context, queries []string) error {
	for _, q := range queries {

		err := ip.execute(ctx, q)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error executing query: %v", err)
			return err
		}
	}
	return nil
}

func (ip *IngestProcessor2) preprocessJsons(ctx context.Context,
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
		stats.GlobalStatistics.UpdateNonSchemaValues(ip.cfg, tableName,
			inValidJson, NestedSeparator)
		// Remove invalid fields from the input JSON
		jsonValue = subtractInputJson(jsonValue, inValidJson)
		validatedJsons = append(validatedJsons, jsonValue)
	}
	return validatedJsons, invalidJsons, nil
}

func (ip *IngestProcessor2) FindTable(tableName string) (result *chLib.Table) {
	tableNamePattern := util.TableNamePatternRegexp(tableName)
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

func (ip *IngestProcessor2) storeVirtualTable(table *chLib.Table) error {

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
func (ip *IngestProcessor2) AddTableIfDoesntExist(table *chLib.Table) bool {
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

func (ip *IngestProcessor2) Ping() error {
	return ip.chDb.Open()
}

func NewIngestProcessor2(cfg *config.QuesmaConfiguration, chDb quesma_api.BackendConnector, phoneHomeAgent telemetry.PhoneHomeAgent, loader chLib.TableDiscovery, schemaRegistry schema.Registry, virtualTableStorage persistence.JSONDatabase, esBackendConn backend_connectors.ElasticsearchBackendConnector) *IngestProcessor2 {
	ctx, cancel := context.WithCancel(context.Background())
	return &IngestProcessor2{ctx: ctx, cancel: cancel, chDb: chDb, tableDiscovery: loader, cfg: cfg, phoneHomeAgent: phoneHomeAgent, schemaRegistry: schemaRegistry, virtualTableStorage: virtualTableStorage, es: esBackendConn}
}

// validateIngest validates the document against the table schema
// and returns the fields that are not valid e.g. have wrong types
// according to the schema
func (ip *IngestProcessor2) validateIngest(tableName string, document types.JSON) (types.JSON, error) {
	clickhouseTable := ip.FindTable(tableName)

	if clickhouseTable == nil {
		logger.Error().Msgf("Table %s not found", tableName)
		return nil, errors.New("table not found:" + tableName)
	}
	deletedFields := make(types.JSON)
	for columnName, column := range clickhouseTable.Cols {
		if column == nil {
			continue
		}
		if value, ok := document[columnName]; ok {
			if value == nil {
				continue
			}
			for k, v := range validateValueAgainstType(columnName, value, column) {
				deletedFields[k] = v
			}
		}
	}
	return deletedFields, nil
}
