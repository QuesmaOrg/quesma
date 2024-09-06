// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"math"
	"quesma/concurrent"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/index"
	"quesma/jsonprocessor"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/stats"
	"quesma/telemetry"
	"quesma/util"
	"slices"
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

type (
	// LogManager should be renamed to Connector  -> TODO !!!
	LogManager struct {
		ctx                       context.Context
		cancel                    context.CancelFunc
		chDb                      *sql.DB
		tableDiscovery            TableDiscovery
		cfg                       *config.QuesmaConfiguration
		phoneHomeAgent            telemetry.PhoneHomeAgent
		schemaRegistry            schema.Registry
		ingestCounter             int64
		ingestFieldStatistics     IngestFieldStatistics
		ingestFieldStatisticsLock sync.Mutex
	}
	TableMap  = concurrent.Map[string, *Table]
	SchemaMap = map[string]interface{} // TODO remove
	Attribute struct {
		KeysArrayName   string
		ValuesArrayName string
		TypesArrayName  string
		MapValueName    string
		MapMetadataName string
		Type            BaseType
	}
	ChTableConfig struct {
		hasTimestamp bool // does table have 'timestamp' field
		// allow_suspicious_ttl_expressions=1 to enable TTL without date field (doesn't work for me!)
		// also be very cautious with it and test it beforehand, people say it doesn't work properly
		// TODO make sure it's unique in schema (there's no other 'timestamp' field)
		// I (Krzysiek) can write it quickly, but don't want to waste time for it right now.
		timestampDefaultsNow bool
		engine               string // "Log", "MergeTree", etc.
		orderBy              string // "" if none
		partitionBy          string // "" if none
		primaryKey           string // "" if none
		settings             string // "" if none
		ttl                  string // of type Interval, e.g. 3 MONTH, 1 YEAR
		// look https://clickhouse.com/docs/en/sql-reference/data-types/special-data-types/interval
		// "" if none
		// TODO make sure it's unique in schema (there's no other 'others' field)
		// I (Krzysiek) can write it quickly, but don't want to waste time for it right now.
		attributes                            []Attribute
		castUnsupportedAttrValueTypesToString bool // if we have e.g. only attrs (String, String), we'll cast e.g. Date to String
		preferCastingToOthers                 bool // we'll put non-schema field in [String, String] attrs map instead of others, if we have both options
	}
)

func NewTableMap() *TableMap {
	return concurrent.NewMap[string, *Table]()
}

func (lm *LogManager) Start() {
	if err := lm.chDb.Ping(); err != nil {
		endUserError := end_user_errors.GuessClickhouseErrorType(err)
		logger.ErrorWithCtxAndReason(lm.ctx, endUserError.Reason()).Msgf("could not connect to clickhouse. error: %v", endUserError)
	}

	lm.tableDiscovery.ReloadTableDefinitions()

	logger.Info().Msgf("schemas loaded: %s", lm.tableDiscovery.TableDefinitions().Keys())
	const reloadInterval = 1 * time.Minute
	forceReloadCh := lm.tableDiscovery.ForceReloadCh()

	go func() {
		recovery.LogPanic()
		for {
			select {
			case <-lm.ctx.Done():
				logger.Debug().Msg("closing log manager")
				return
			case doneCh := <-forceReloadCh:
				// this prevents flood of reloads, after a long pause
				if time.Since(lm.tableDiscovery.LastReloadTime()) > reloadInterval {
					lm.tableDiscovery.ReloadTableDefinitions()
				}
				doneCh <- struct{}{}
			case <-time.After(reloadInterval):
				// only reload if we actually use Quesma, make it double time to prevent edge case
				// otherwise it prevent ClickHouse Cloud from idle pausing
				if time.Since(lm.tableDiscovery.LastAccessTime()) < reloadInterval*2 {
					lm.tableDiscovery.ReloadTableDefinitions()
				}
			}
		}
	}()
}

func (lm *LogManager) Stop() {
	lm.cancel()
}

type discoveredTable struct {
	name             string
	columnTypes      map[string]string
	config           config.IndexConfiguration
	comment          string
	createTableQuery string
}

func (lm *LogManager) ReloadTables() {
	logger.Info().Msg("reloading tables definitions")
	lm.tableDiscovery.ReloadTableDefinitions()
}

func (lm *LogManager) Close() {
	_ = lm.chDb.Close()
}

// Deprecated: use ResolveIndexes instead, this method will be removed once we switch to the new one
// Indexes can be in a form of wildcard, e.g. "index-*"
// If we have such index, we need to resolve it to a real table name.
func (lm *LogManager) ResolveTableName(index string) (result string) {
	lm.tableDiscovery.TableDefinitions().
		Range(func(k string, v *Table) bool {
			if elasticsearch.IndexMatches(index, k) {
				result = k
				return false
			}
			return true
		})
	return result
}

// Indexes can be in a form of wildcard, e.g. "index-*" or even contain multiple patterns like "index-*,logs-*",
// this method returns all matching indexes
// empty pattern means all indexes
// "_all" index name means all indexes
func (lm *LogManager) ResolveIndexes(ctx context.Context, patterns string) (results []string, err error) {
	if err = lm.tableDiscovery.TableDefinitionsFetchError(); err != nil {
		return nil, err
	}

	results = make([]string, 0)
	if strings.Contains(patterns, ",") {
		for _, pattern := range strings.Split(patterns, ",") {
			if pattern == elasticsearch.AllIndexesAliasIndexName || pattern == "" {
				results = lm.tableDiscovery.TableDefinitions().Keys()
				slices.Sort(results)
				return results, nil
			} else {
				indexes, err := lm.ResolveIndexes(ctx, pattern)
				if err != nil {
					return nil, err
				}
				results = append(results, indexes...)
			}
		}
	} else {
		if patterns == elasticsearch.AllIndexesAliasIndexName || len(patterns) == 0 {
			results = lm.tableDiscovery.TableDefinitions().Keys()
			slices.Sort(results)
			return results, nil
		} else {
			lm.tableDiscovery.TableDefinitions().
				Range(func(tableName string, v *Table) bool {
					if elasticsearch.IndexMatches(patterns, tableName) {
						results = append(results, tableName)
					}
					return true
				})
		}
	}

	return util.Distinct(results), nil
}

// updates also Table TODO stop updating table here, find a better solution
func addOurFieldsToCreateTableQuery(q string, config *ChTableConfig, table *Table) string {
	if len(config.attributes) == 0 {
		_, ok := table.Cols[timestampFieldName]
		if !config.hasTimestamp || ok {
			return q
		}
	}

	othersStr, timestampStr, attributesStr := "", "", ""
	if config.hasTimestamp {
		_, ok := table.Cols[timestampFieldName]
		if !ok {
			defaultStr := ""
			if config.timestampDefaultsNow {
				defaultStr = " DEFAULT now64()"
			}
			timestampStr = fmt.Sprintf("%s\"%s\" DateTime64(3)%s,\n", util.Indent(1), timestampFieldName, defaultStr)
			table.Cols[timestampFieldName] = &Column{Name: timestampFieldName, Type: NewBaseType("DateTime64")}
		}
	}
	if len(config.attributes) > 0 {
		for _, a := range config.attributes {
			_, ok := table.Cols[a.MapValueName]
			if !ok {
				attributesStr += fmt.Sprintf("%s\"%s\" Map(String,String),\n", util.Indent(1), a.MapValueName)
				table.Cols[a.MapValueName] = &Column{Name: a.MapValueName, Type: CompoundType{Name: "Map", BaseType: NewBaseType("String, String")}}
			}
			_, ok = table.Cols[a.MapMetadataName]
			if !ok {
				attributesStr += fmt.Sprintf("%s\"%s\" Map(String,String),\n", util.Indent(1), a.MapMetadataName)
				table.Cols[a.MapMetadataName] = &Column{Name: a.MapMetadataName, Type: CompoundType{Name: "Map", BaseType: NewBaseType("String, String")}}
			}
		}
	}

	i := strings.Index(q, "(")
	return q[:i+2] + othersStr + timestampStr + attributesStr + q[i+1:]
}

func (lm *LogManager) CountMultiple(ctx context.Context, tables ...string) (int64, error) {
	if len(tables) == 0 {
		return 0, nil
	}
	const subcountStatement = "(SELECT count(*) FROM ?)"
	var subCountStatements []string
	for range len(tables) {
		subCountStatements = append(subCountStatements, subcountStatement)
	}

	var count int64
	var anyTables []any
	for _, t := range tables {
		anyTables = append(anyTables, t)
	}
	err := lm.chDb.QueryRowContext(ctx, fmt.Sprintf("SELECT sum(*) as count FROM (%s)", strings.Join(subCountStatements, " UNION ALL ")), anyTables...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	return count, nil
}

func (lm *LogManager) Count(ctx context.Context, table string) (int64, error) {
	var count int64
	err := lm.chDb.QueryRowContext(ctx, "SELECT count(*) FROM ?", table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	return count, nil
}

func (lm *LogManager) executeRawQuery(query string) (*sql.Rows, error) {
	if res, err := lm.chDb.Query(query); err != nil {
		return nil, fmt.Errorf("error in executeRawQuery: query: %s\nerr:%v", query, err)
	} else {
		return res, nil
	}
}

/* The logic below contains a simple checks that are executed by connectors to ensure that they are
not connected to the data sources which are not allowed by current license. */

type PaidServiceName int

const (
	CHCloudServiceName PaidServiceName = iota
	HydrolixServiceName
)

func (s PaidServiceName) String() string {
	return [...]string{"ClickHouse Cloud", "Hydrolix"}[s]
}

var paidServiceChecks = map[PaidServiceName]string{
	HydrolixServiceName: `SELECT concat(database,'.', table) FROM system.tables WHERE engine = 'TurbineStorage';`,
	CHCloudServiceName:  `SELECT concat(database,'.', table) FROM system.tables WHERE engine = 'SharedMergeTree';`,
	// For CH Cloud we can also check the output of the following query: --> `SELECT * FROM system.settings WHERE name='cloud_mode_engine';`
}

func (lm *LogManager) isConnectedToPaidService(service PaidServiceName) (bool, error) {
	rows, err := lm.executeRawQuery(paidServiceChecks[service])
	if err != nil {
		return false, fmt.Errorf("error executing %s-identifying query: %v", service, err)
	}
	defer rows.Close()
	if rows.Next() {
		return true, fmt.Errorf("detected %s-specific table engine, which is not allowed", service)
	}
	return false, nil
}

// CheckIfConnectedPaidService executes simple query with exponential backoff
func (lm *LogManager) CheckIfConnectedPaidService(service PaidServiceName) (returnedErr error) {
	if _, ok := paidServiceChecks[service]; !ok {
		return fmt.Errorf("service %s is not supported", service)
	}
	totalCheckTime := time.Minute
	startTimeInSeconds := 2.0
	start := time.Now()
	attempt := 0
	for {
		isConnectedToPaidService, err := lm.isConnectedToPaidService(service)
		if err != nil {
			returnedErr = fmt.Errorf("error checking connection to database, attempt #%d, err=%v", attempt+1, err)
		}
		if isConnectedToPaidService {
			return fmt.Errorf("detected %s-specific table engine, which is not allowed", service)
		} else if err == nil { // no paid service detected, no conn errors
			returnedErr = nil
			break
		}
		if time.Since(start) > totalCheckTime {
			break
		}
		attempt++
		sleepDuration := time.Duration(math.Pow(startTimeInSeconds, float64(attempt))) * time.Second
		if remaining := time.Until(start.Add(totalCheckTime)); remaining < sleepDuration {
			sleepDuration = remaining
		}
		time.Sleep(sleepDuration)
	}
	return returnedErr
}

func (lm *LogManager) ProcessCreateTableQuery(ctx context.Context, query string, config *ChTableConfig) error {
	table, err := NewTable(query, config)
	if err != nil {
		return err
	}

	// if exists only then createTable
	noSuchTable := lm.AddTableIfDoesntExist(table)
	if !noSuchTable {
		return fmt.Errorf("table %s already exists", table.Name)
	}

	return lm.execute(ctx, addOurFieldsToCreateTableQuery(query, config, table))
}

func findSchemaPointer(schemaRegistry schema.Registry, tableName string) *schema.Schema {
	if foundSchema, found := schemaRegistry.FindSchema(schema.TableName(tableName)); found {
		return &foundSchema
	}
	return nil
}

func (lm *LogManager) getIgnoredFields(tableName string) []config.FieldName {
	if indexConfig, found := lm.cfg.IndexConfig[tableName]; found && indexConfig.SchemaOverrides != nil {
		// FIXME: don't get ignored fields from schema config, but store
		// them in the schema registry - that way we don't have to manually replace '.' with '::'
		// in removeFieldsTransformer's Transform method
		return indexConfig.SchemaOverrides.IgnoredFields()
	}
	return nil
}

func (lm *LogManager) buildCreateTableQueryNoOurFields(ctx context.Context, tableName string,
	jsonData types.JSON, tableConfig *ChTableConfig, nameFormatter TableColumNameFormatter) ([]CreateTableEntry, map[schema.FieldName]CreateTableEntry) {
	ignoredFields := lm.getIgnoredFields(tableName)

	columnsFromJson := JsonToColumns("", jsonData, 1,
		tableConfig, nameFormatter, ignoredFields)
	columnsFromSchema := SchemaToColumns(findSchemaPointer(lm.schemaRegistry, tableName), nameFormatter)
	return columnsFromJson, columnsFromSchema
}

func Indexes(m SchemaMap) string {
	var result strings.Builder
	for col := range m {
		index := getIndexStatement(col)
		if index != "" {
			result.WriteString(",\n")
			result.WriteString(util.Indent(1))
			result.WriteString(index.statement())
		}
	}
	result.WriteString(",\n")
	return result.String()
}

func createTableQuery(name string, columns string, config *ChTableConfig) string {
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
		newAttrsMap[DeprecatedAttributesKeyColumn] = append(newAttrsMap[DeprecatedAttributesKeyColumn], k)
		newAttrsMap[DeprecatedAttributesValueColumn] = append(newAttrsMap[DeprecatedAttributesValueColumn], v)
		newAttrsMap[DeprecatedAttributesValueType] = append(newAttrsMap[DeprecatedAttributesValueType], NewType(v).String())
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
func (lm *LogManager) generateNewColumns(
	attrsMap map[string][]interface{},
	table *Table,
	alteredAttributesIndexes []int) []string {
	var alterCmd []string
	attrKeys := getAttributesByArrayName(DeprecatedAttributesKeyColumn, attrsMap)
	attrTypes := getAttributesByArrayName(DeprecatedAttributesValueType, attrsMap)
	var deleteIndexes []int

	// HACK Alert:
	// We must avoid altering the table.Cols map and reading at the same time.
	// This should be protected by a lock or a copy of the table should be used.
	//
	newColumns := make(map[string]*Column)
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
		alterTable := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table.Name, attrKeys[i], columnType)

		newColumns[attrKeys[i]] = &Column{Name: attrKeys[i], Type: NewBaseType(attrTypes[i]), Modifiers: modifiers}
		alterCmd = append(alterCmd, alterTable)
		deleteIndexes = append(deleteIndexes, i)
	}

	table.Cols = newColumns

	for i := len(deleteIndexes) - 1; i >= 0; i-- {
		attrsMap[DeprecatedAttributesKeyColumn] = append(attrsMap[DeprecatedAttributesKeyColumn][:deleteIndexes[i]], attrsMap[DeprecatedAttributesKeyColumn][deleteIndexes[i]+1:]...)
		attrsMap[DeprecatedAttributesValueType] = append(attrsMap[DeprecatedAttributesValueType][:deleteIndexes[i]], attrsMap[DeprecatedAttributesValueType][deleteIndexes[i]+1:]...)
		attrsMap[DeprecatedAttributesValueColumn] = append(attrsMap[DeprecatedAttributesValueColumn][:deleteIndexes[i]], attrsMap[DeprecatedAttributesValueColumn][deleteIndexes[i]+1:]...)
	}
	return alterCmd
}

func generateNonSchemaFieldsString(attrsMap map[string][]interface{}) (string, error) {
	var nonSchemaStr string
	if len(attrsMap) <= 0 {
		return nonSchemaStr, nil
	}
	attrKeys := getAttributesByArrayName(DeprecatedAttributesKeyColumn, attrsMap)
	attrValues := getAttributesByArrayName(DeprecatedAttributesValueColumn, attrsMap)
	attrTypes := getAttributesByArrayName(DeprecatedAttributesValueType, attrsMap)

	attributesColumns := []string{AttributesValuesColumn, AttributesMetadataColumn}

	for columnIndex, column := range attributesColumns {
		var value string
		if columnIndex > 0 {
			nonSchemaStr += ","
		}
		nonSchemaStr += "\"" + column + "\":{"
		for i := 0; i < len(attrKeys); i++ {
			if columnIndex > 0 {
				// We are versioning metadata fields
				// At the moment we store only types
				// but that might change in the future
				const metadataVersionPrefix = "v1"
				value = metadataVersionPrefix + ";" + attrTypes[i]
			} else {
				value = attrValues[i]
			}
			if i > 0 {
				nonSchemaStr += ","
			}
			nonSchemaStr += fmt.Sprintf("\"%s\":\"%s\"", attrKeys[i], value)
		}
		nonSchemaStr = nonSchemaStr + "}"

	}
	return nonSchemaStr, nil
}

// This function implements heuristic for deciding if we should add new columns
func (lm *LogManager) shouldAlterColumns(table *Table, attrsMap map[string][]interface{}) (bool, []int) {
	attrKeys := getAttributesByArrayName(DeprecatedAttributesKeyColumn, attrsMap)
	alterColumnIndexes := make([]int, 0)
	if len(table.Cols) < alwaysAddColumnLimit {
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
	lm.ingestFieldStatisticsLock.Lock()
	if lm.ingestFieldStatistics == nil {
		lm.ingestFieldStatistics = make(IngestFieldStatistics)
	}
	lm.ingestFieldStatisticsLock.Unlock()
	for i := 0; i < len(attrKeys); i++ {
		lm.ingestFieldStatisticsLock.Lock()
		lm.ingestFieldStatistics[IngestFieldBucketKey{indexName: table.Name, field: attrKeys[i]}]++
		counter := atomic.LoadInt64(&lm.ingestCounter)
		fieldCounter := lm.ingestFieldStatistics[IngestFieldBucketKey{indexName: table.Name, field: attrKeys[i]}]
		// reset statistics every alwaysAddColumnLimit
		// for now alwaysAddColumnLimit is used in two contexts
		// for defining column limit and for resetting statistics
		if counter >= alwaysAddColumnLimit {
			atomic.StoreInt64(&lm.ingestCounter, 0)
			lm.ingestFieldStatistics = make(IngestFieldStatistics)
		}
		lm.ingestFieldStatisticsLock.Unlock()
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

func (lm *LogManager) BuildIngestSQLStatements(tableName string, data types.JSON, inValidJson types.JSON,
	config *ChTableConfig) (string, []string, error) {

	jsonData, err := json.Marshal(data)

	if err != nil {
		return "", nil, err
	}
	jsonDataAsString := string(jsonData)

	// we find all non-schema fields
	jsonMap, err := types.ParseJSON(jsonDataAsString)
	if err != nil {
		return "", nil, err
	}

	wasReplaced := replaceDotsWithSeparator(jsonMap)

	if len(config.attributes) == 0 {
		// if we don't have any attributes, and it wasn't replaced,
		// we don't need to modify the json
		if !wasReplaced {
			return jsonDataAsString, nil, nil
		}
		rawBytes, err := jsonMap.Bytes()
		if err != nil {
			return "", nil, err
		}
		return string(rawBytes), nil, nil
	}

	table := lm.FindTable(tableName)
	schemaFieldsJson, err := json.Marshal(jsonMap)

	if err != nil {
		return "", nil, err
	}

	mDiff := DifferenceMap(jsonMap, table) // TODO change to DifferenceMap(m, t)

	if len(mDiff) == 0 && string(schemaFieldsJson) == jsonDataAsString && len(inValidJson) == 0 { // no need to modify, just insert 'js'
		return jsonDataAsString, nil, nil
	}

	// check attributes precondition
	if len(config.attributes) <= 0 {
		return "", nil, fmt.Errorf("no attributes config, but received non-schema fields: %s", mDiff)
	}
	attrsMap, _ := BuildAttrsMap(mDiff, config)

	// generateNewColumns is called on original attributes map
	// before adding invalid fields to it
	// otherwise it would contain invalid fields e.g. with wrong types
	// we only want to add fields that are not part of the schema e.g we don't
	// have columns for them
	var alterCmd []string
	atomic.AddInt64(&lm.ingestCounter, 1)
	if ok, alteredAttributesIndexes := lm.shouldAlterColumns(table, attrsMap); ok {
		alterCmd = lm.generateNewColumns(attrsMap, table, alteredAttributesIndexes)
	}
	// If there are some invalid fields, we need to add them to the attributes map
	// to not lose them and be able to store them later by
	// generating correct update query
	// addInvalidJsonFieldsToAttributes returns a new map with invalid fields added
	// this map is then used to generate non-schema fields string
	attrsMapWithInvalidFields := addInvalidJsonFieldsToAttributes(attrsMap, inValidJson)
	nonSchemaStr, err := generateNonSchemaFieldsString(attrsMapWithInvalidFields)

	if err != nil {
		return "", nil, err
	}

	onlySchemaFields := RemoveNonSchemaFields(jsonMap, table)

	schemaFieldsJson, err = json.Marshal(onlySchemaFields)

	if err != nil {
		return "", nil, err
	}
	comma := ""
	if nonSchemaStr != "" && len(schemaFieldsJson) > 2 {
		comma = "," // need to watch out where we input commas, CH doesn't tolerate trailing ones
	}

	return fmt.Sprintf("{%s%s%s", nonSchemaStr, comma, schemaFieldsJson[1:]), alterCmd, nil
}

func (lm *LogManager) processInsertQuery(ctx context.Context,
	tableName string,
	jsonData []types.JSON, transformer jsonprocessor.IngestTransformer,
	tableFormatter TableColumNameFormatter,
) ([]string, error) {
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
	// TODO this is doing nested field encoding
	// ----------------------
	table := lm.FindTable(tableName)
	var config *ChTableConfig
	if table == nil {
		config = NewOnlySchemaFieldsCHConfig()
		ignoredFields := lm.getIgnoredFields(tableName)
		columnsFromJson := JsonToColumns("", jsonData[0], 1,
			config, tableFormatter, ignoredFields)
		columnsFromSchema := SchemaToColumns(findSchemaPointer(lm.schemaRegistry, tableName), tableFormatter)
		columns := columnsWithIndexes(columnsToString(columnsFromJson, columnsFromSchema), Indexes(jsonData[0]))
		createTableCmd := createTableQuery(tableName, columns, config)
		err := lm.ProcessCreateTableQuery(ctx, createTableCmd, config)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error ProcessInsertQuery, can't create table: %v", err)
			return nil, err
		}
	} else if !table.Created {
		err := lm.execute(ctx, table.createTableString())
		if err != nil {
			return nil, err
		}
		config = table.Config
	} else {
		config = table.Config
	}
	// ----------------------

	// TODO this is doing nested field encoding
	// ----------------------
	return lm.GenerateSqlStatements(ctx, tableName, jsonData, config, transformer)
	// ----------------------
}

func (lm *LogManager) ProcessInsertQuery(ctx context.Context, tableName string,
	jsonData []types.JSON, transformer jsonprocessor.IngestTransformer,
	tableFormatter TableColumNameFormatter) error {
	statements, err := lm.processInsertQuery(ctx, tableName, jsonData, transformer, tableFormatter)
	if err != nil {
		return err
	}
	// We expect to have date format set to `best_effort`
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"date_time_input_format": "best_effort",
	}))
	return lm.executeStatements(ctx, statements)
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
func (lm *LogManager) execute(ctx context.Context, query string) error {
	span := lm.phoneHomeAgent.ClickHouseInsertDuration().Begin()

	// We log every DDL query
	if strings.HasPrefix(query, "ALTER") || strings.HasPrefix(query, "CREATE") {
		logger.InfoWithCtx(ctx).Msgf("DDL query execution: %s", query)
	}

	_, err := lm.chDb.ExecContext(ctx, query)
	span.End(err)
	return err
}

func (lm *LogManager) executeStatements(ctx context.Context, queries []string) error {
	for _, q := range queries {
		err := lm.execute(ctx, q)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error executing query: %v", err)
			return err
		}
	}
	return nil
}

func (lm *LogManager) GenerateSqlStatements(ctx context.Context, tableName string, jsons []types.JSON,
	config *ChTableConfig, transformer jsonprocessor.IngestTransformer) ([]string, error) {

	var jsonsReadyForInsertion []string
	var alterCmd []string
	var preprocessedJsons []types.JSON
	var invalidJsons []types.JSON
	for _, jsonValue := range jsons {
		preprocessedJson, err := transformer.Transform(jsonValue)
		if err != nil {
			return nil, fmt.Errorf("error IngestTransformer: %v", err)
		}
		// Validate the input JSON
		// against the schema
		inValidJson, err := lm.validateIngest(tableName, preprocessedJson)
		if err != nil {
			return nil, fmt.Errorf("error validation: %v", err)
		}
		invalidJsons = append(invalidJsons, inValidJson)
		stats.GlobalStatistics.UpdateNonSchemaValues(lm.cfg, tableName,
			inValidJson, NestedSeparator)
		// Remove invalid fields from the input JSON
		preprocessedJson = subtractInputJson(preprocessedJson, inValidJson)
		preprocessedJsons = append(preprocessedJsons, preprocessedJson)
	}
	for i, preprocessedJson := range preprocessedJsons {
		// TODO this is doing nested field encoding
		// ----------------------
		insertJson, alter, err := lm.BuildIngestSQLStatements(tableName, preprocessedJson, invalidJsons[i], config)
		// ----------------------
		alterCmd = append(alterCmd, alter...)
		if err != nil {
			return nil, fmt.Errorf("error BuildInsertJson, tablename: '%s' json: '%s': %v", tableName, PrettyJson(insertJson), err)
		}
		jsonsReadyForInsertion = append(jsonsReadyForInsertion, insertJson)
	}

	insertValues := strings.Join(jsonsReadyForInsertion, ", ")
	insert := fmt.Sprintf("INSERT INTO \"%s\" FORMAT JSONEachRow %s", tableName, insertValues)

	var statements []string
	statements = append(statements, alterCmd...)
	statements = append(statements, insert)
	return statements, nil
}

func (lm *LogManager) FindTable(tableName string) (result *Table) {
	tableNamePattern := index.TableNamePatternRegexp(tableName)
	lm.tableDiscovery.TableDefinitions().
		Range(func(name string, table *Table) bool {
			if tableNamePattern.MatchString(name) {
				result = table
				return false
			}
			return true
		})

	return result
}

func (lm *LogManager) GetTableDefinitions() (TableMap, error) {
	if err := lm.tableDiscovery.TableDefinitionsFetchError(); err != nil {
		return *lm.tableDiscovery.TableDefinitions(), err
	}

	return *lm.tableDiscovery.TableDefinitions(), nil
}

// Returns if schema wasn't created (so it needs to be, and will be in a moment)
func (lm *LogManager) AddTableIfDoesntExist(table *Table) bool {
	t := lm.FindTable(table.Name)
	if t == nil {
		table.Created = true

		table.applyIndexConfig(lm.cfg)

		lm.tableDiscovery.TableDefinitions().Store(table.Name, table)
		return true
	}
	wasntCreated := !t.Created
	t.Created = true
	return wasntCreated
}

func (lm *LogManager) Ping() error {
	return lm.chDb.Ping()
}

func NewEmptyLogManager(cfg *config.QuesmaConfiguration, chDb *sql.DB, phoneHomeAgent telemetry.PhoneHomeAgent, loader TableDiscovery, schemaRegistry schema.Registry) *LogManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &LogManager{ctx: ctx, cancel: cancel, chDb: chDb, tableDiscovery: loader, cfg: cfg, phoneHomeAgent: phoneHomeAgent, schemaRegistry: schemaRegistry}
}

func NewLogManager(tables *TableMap, cfg *config.QuesmaConfiguration) *LogManager {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(tables)
	return &LogManager{chDb: nil, tableDiscovery: newTableDiscoveryWith(cfg, nil, *tables),
		cfg: cfg, phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(),
		ingestFieldStatistics: make(IngestFieldStatistics)}
}

// right now only for tests purposes
func NewLogManagerWithConnection(db *sql.DB, tables *TableMap) *LogManager {
	return &LogManager{chDb: db, tableDiscovery: newTableDiscoveryWith(&config.QuesmaConfiguration{}, db, *tables),
		phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(), ingestFieldStatistics: make(IngestFieldStatistics)}
}

func NewLogManagerEmpty() *LogManager {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	cfg := &config.QuesmaConfiguration{}
	return &LogManager{tableDiscovery: NewTableDiscovery(cfg, nil), cfg: cfg,
		phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(), ingestFieldStatistics: make(IngestFieldStatistics)}
}

func NewOnlySchemaFieldsCHConfig() *ChTableConfig {
	return &ChTableConfig{
		hasTimestamp:                          true,
		timestampDefaultsNow:                  true,
		engine:                                "MergeTree",
		orderBy:                               "(" + `"@timestamp"` + ")",
		partitionBy:                           "",
		primaryKey:                            "",
		ttl:                                   "",
		attributes:                            []Attribute{NewDefaultStringAttribute()},
		castUnsupportedAttrValueTypesToString: false,
		preferCastingToOthers:                 false,
	}
}

func NewDefaultCHConfig() *ChTableConfig {
	return &ChTableConfig{
		hasTimestamp:         true,
		timestampDefaultsNow: true,
		engine:               "MergeTree",
		orderBy:              "(" + `"@timestamp"` + ")",
		partitionBy:          "",
		primaryKey:           "",
		ttl:                  "",
		attributes: []Attribute{
			NewDefaultInt64Attribute(),
			NewDefaultFloat64Attribute(),
			NewDefaultBoolAttribute(),
			NewDefaultStringAttribute(),
		},
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
}

func NewNoTimestampOnlyStringAttrCHConfig() *ChTableConfig {
	return &ChTableConfig{
		hasTimestamp:         false,
		timestampDefaultsNow: false,
		engine:               "MergeTree",
		orderBy:              "(" + `"@timestamp"` + ")",
		partitionBy:          "",
		primaryKey:           "",
		ttl:                  "",
		attributes: []Attribute{
			NewDefaultStringAttribute(),
		},
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
}

func NewChTableConfigNoAttrs() *ChTableConfig {
	return &ChTableConfig{
		hasTimestamp:                          false,
		timestampDefaultsNow:                  false,
		engine:                                "MergeTree",
		orderBy:                               "(" + `"@timestamp"` + ")",
		attributes:                            []Attribute{},
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
}

func NewChTableConfigFourAttrs() *ChTableConfig {
	return &ChTableConfig{
		hasTimestamp:         false,
		timestampDefaultsNow: true,
		engine:               "MergeTree",
		orderBy:              "(" + "`@timestamp`" + ")",
		attributes: []Attribute{
			NewDefaultInt64Attribute(),
			NewDefaultFloat64Attribute(),
			NewDefaultBoolAttribute(),
			NewDefaultStringAttribute(),
		},
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
}

func NewChTableConfigTimestampStringAttr() *ChTableConfig {
	return &ChTableConfig{
		hasTimestamp:                          true,
		timestampDefaultsNow:                  true,
		attributes:                            []Attribute{NewDefaultStringAttribute()},
		engine:                                "MergeTree",
		orderBy:                               "(" + "`@timestamp`" + ")",
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
}

func (c *ChTableConfig) GetAttributes() []Attribute {
	return c.attributes
}
