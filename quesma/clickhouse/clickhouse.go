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
	"quesma/plugins"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/telemetry"
	"quesma/util"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

const (
	timestampFieldName = "@timestamp" // it's always DateTime64 for now, don't want to waste time changing that, we don't seem to use that anyway
	othersFieldName    = "others"
)

type (
	// LogManager should be renamed to Connector  -> TODO !!!
	LogManager struct {
		ctx            context.Context
		cancel         context.CancelFunc
		chDb           *sql.DB
		tableDiscovery TableDiscovery
		cfg            config.QuesmaConfiguration
		phoneHomeAgent telemetry.PhoneHomeAgent
		schemaRegistry schema.Registry
	}
	TableMap  = concurrent.Map[string, *Table]
	SchemaMap = map[string]interface{} // TODO remove
	Attribute struct {
		KeysArrayName   string
		ValuesArrayName string
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
		hasOthers bool // has additional "others" JSON field for out of schema values
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
	if !config.hasOthers && len(config.attributes) == 0 {
		_, ok := table.Cols[timestampFieldName]
		if !config.hasTimestamp || ok {
			return q
		}
	}

	othersStr, timestampStr, attributesStr := "", "", ""
	if config.hasOthers {
		_, ok := table.Cols[othersFieldName]
		if !ok {
			othersStr = fmt.Sprintf("%s\"%s\" JSON,\n", util.Indent(1), othersFieldName)
			table.Cols[othersFieldName] = &Column{Name: othersFieldName, Type: NewBaseType("JSON")}
		}
	}
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
			_, ok := table.Cols[a.KeysArrayName]
			if !ok {
				attributesStr += fmt.Sprintf("%s\"%s\" Array(String),\n", util.Indent(1), a.KeysArrayName)
				table.Cols[a.KeysArrayName] = &Column{Name: a.KeysArrayName, Type: CompoundType{Name: "Array", BaseType: NewBaseType("String")}}
			}
			_, ok = table.Cols[a.ValuesArrayName]
			if !ok {
				attributesStr += fmt.Sprintf("%s\"%s\" Array(%s),\n", util.Indent(1), a.ValuesArrayName, a.Type.String())
				table.Cols[a.ValuesArrayName] = &Column{Name: a.ValuesArrayName, Type: a.Type}
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

func (lm *LogManager) sendCreateTableQuery(ctx context.Context, query string) error {
	if _, err := lm.chDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("error in sendCreateTableQuery: query: %s\nerr:%v", query, err)
	}
	return nil
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

	return lm.sendCreateTableQuery(ctx, addOurFieldsToCreateTableQuery(query, config, table))
}

func findSchemaPointer(schemaRegistry schema.Registry, tableName string) *schema.Schema {
	if foundSchema, found := schemaRegistry.FindSchema(schema.TableName(tableName)); found {
		return &foundSchema
	}
	return nil
}

func (lm *LogManager) buildCreateTableQueryNoOurFields(ctx context.Context, tableName string,
	jsonData types.JSON, tableConfig *ChTableConfig, nameFormatter plugins.TableColumNameFormatter) (string, error) {

	columns := FieldsMapToCreateTableString(jsonData, tableConfig, nameFormatter, findSchemaPointer(lm.schemaRegistry, tableName)) + Indexes(jsonData)

	createTableCmd := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"
(

%s
)
%s
COMMENT 'created by Quesma'`,
		tableName, columns,
		tableConfig.CreateTablePostFieldsString())

	return createTableCmd, nil
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

func (lm *LogManager) CreateTableFromInsertQuery(ctx context.Context, name string, jsonData types.JSON, config *ChTableConfig, tableFormatter plugins.TableColumNameFormatter) error {
	// TODO fix lm.AddTableIfDoesntExist(name, jsonData)

	query, err := lm.buildCreateTableQueryNoOurFields(ctx, name, jsonData, config, tableFormatter)
	if err != nil {
		return err
	}

	err = lm.ProcessCreateTableQuery(ctx, query, config)
	if err != nil {
		return err
	}
	return nil
}

// This function takes an attributesMap and updates it
// with the fields that are not valid according to the inferred schema
func addInvalidJsonFieldsToAttributes(attrsMap map[string][]interface{}, invalidJson types.JSON) {
	for k, v := range inValidJson {
		attrsMap[AttributesKeyColumn] = append(attrsMap[AttributesKeyColumn], k)
		attrsMap[AttributesValueColumn] = append(attrsMap[AttributesValueColumn], v)
	}
}

// TODO
// This method should be refactored to use mux.JSON instead of string
func (lm *LogManager) BuildInsertJson(tableName string, data types.JSON, inValidJson types.JSON, config *ChTableConfig) (string, error) {

	jsonData, err := json.Marshal(data)

	if err != nil {
		return "", err
	}
	js := string(jsonData)

	// we find all non-schema fields
	m, err := types.ParseJSON(js)
	if err != nil {
		return "", err
	}

	wasReplaced := replaceDotsWithSeparator(m)
	if !config.hasOthers && len(config.attributes) == 0 {
		if wasReplaced {
			rawBytes, err := m.Bytes()
			if err != nil {
				return "", err
			}
			js = string(rawBytes)
		}
		return js, nil
	}

	t := lm.FindTable(tableName)
	onlySchemaFields := RemoveTypeMismatchSchemaFields(m, t)
	schemaFieldsJson, err := json.Marshal(onlySchemaFields)

	if err != nil {
		return "", err
	}

	mDiff := DifferenceMap(m, t) // TODO change to DifferenceMap(m, t)

	if len(mDiff) == 0 && string(schemaFieldsJson) == js && len(inValidJson) == 0 { // no need to modify, just insert 'js'
		return js, nil
	}
	var attrsMap map[string][]interface{}
	var othersMap SchemaMap
	if len(config.attributes) > 0 {
		attrsMap, othersMap, _ = BuildAttrsMapAndOthers(mDiff, config)
	} else if config.hasOthers {
		othersMap = mDiff
	} else {
		return "", fmt.Errorf("no attributes or others in config, but received non-schema fields: %s", mDiff)
	}
	// If there are some non-valid fields, we need to add them to the attributes map
	// to not lose them and be able to store them later by
	// generating correct update query
	addInvalidJsonFieldsToAttributes(attrsMap, inValidJson)
	nonSchemaStr := ""
	if len(attrsMap) > 0 {
		attrs, err := json.Marshal(attrsMap) // check probably bad, they need to be arrays
		if err != nil {
			return "", err
		}
		nonSchemaStr = string(attrs[1 : len(attrs)-1])
	}
	if len(othersMap) > 0 {
		others, err := json.Marshal(othersMap)
		if err != nil {
			return "", err
		}
		if nonSchemaStr != "" {
			nonSchemaStr += "," // need to watch out where we input commas, CH doesn't tolerate trailing ones
		}
		nonSchemaStr += fmt.Sprintf(`"%s":%s`, othersFieldName, others)
	}
	onlySchemaFields = RemoveNonSchemaFields(m, t)
	schemaFieldsJson, err = json.Marshal(onlySchemaFields)
	if err != nil {
		return "", err
	}
	comma := ""
	if nonSchemaStr != "" && len(schemaFieldsJson) > 2 {
		comma = "," // need to watch out where we input commas, CH doesn't tolerate trailing ones
	}
	return fmt.Sprintf("{%s%s%s", nonSchemaStr, comma, schemaFieldsJson[1:]), nil
}

func (lm *LogManager) GetOrCreateTableConfig(ctx context.Context, tableName string, jsonData types.JSON, tableFormatter plugins.TableColumNameFormatter) (*ChTableConfig, error) {
	table := lm.FindTable(tableName)
	var config *ChTableConfig
	if table == nil {
		config = NewOnlySchemaFieldsCHConfig()
		err := lm.CreateTableFromInsertQuery(ctx, tableName, jsonData, config, tableFormatter)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error ProcessInsertQuery, can't create table: %v", err)
			return nil, err
		}
		return config, nil
	} else if !table.Created {
		err := lm.sendCreateTableQuery(ctx, table.createTableString())
		if err != nil {
			return nil, err
		}
		config = table.Config
	} else {
		config = table.Config
	}
	return config, nil
}

func (lm *LogManager) ProcessInsertQuery(ctx context.Context, tableName string, jsonData []types.JSON, transformer plugins.IngestTransformer, tableFormatter plugins.TableColumNameFormatter) error {

	// this is pre ingest transformer
	// here we transform the data before it's structure evaluation and insertion
	//
	preIngestTransformer := &jsonprocessor.RewriteArrayOfObject{}
	var processed []types.JSON
	for _, jsonValue := range jsonData {
		result, err := preIngestTransformer.Transform(jsonValue)
		if err != nil {
			return fmt.Errorf("error while rewriting json: %v", err)
		}
		processed = append(processed, result)
	}
	jsonData = processed

	tableConfig, err := lm.GetOrCreateTableConfig(ctx, tableName, jsonData[0], tableFormatter)
	if err != nil {
		return err
	}
	return lm.Insert(ctx, tableName, jsonData, tableConfig, transformer)

}

// This function removes fields that are part of anotherDoc from inputDoc
func subtractInputJson(inputDoc types.JSON, anotherDoc types.JSON) types.JSON {
	for key := range anotherDoc {
		delete(inputDoc, key)
	}
	return inputDoc
}

func (lm *LogManager) Insert(ctx context.Context, tableName string, jsons []types.JSON, config *ChTableConfig, transformer plugins.IngestTransformer) error {
	var jsonsReadyForInsertion []string
	for _, jsonValue := range jsons {
		preprocessedJson, err := transformer.Transform(jsonValue)

		if err != nil {
			return fmt.Errorf("error IngestTransformer: %v", err)
		}

		// Validate the input JSON
		// against the schema
		inValidJson, err := lm.validateIngest(tableName, preprocessedJson)
		if err != nil {
			return fmt.Errorf("error validation: %v", err)
		}

		// Remove invalid fields from the input JSON
		preprocessedJson = subtractInputJson(preprocessedJson, inValidJson)

		insertJson, err := lm.BuildInsertJson(tableName, preprocessedJson, inValidJson, config)
		if err != nil {
			return fmt.Errorf("error BuildInsertJson, tablename: '%s' json: '%s': %v", tableName, PrettyJson(insertJson), err)
		}
		jsonsReadyForInsertion = append(jsonsReadyForInsertion, insertJson)
	}

	insertValues := strings.Join(jsonsReadyForInsertion, ", ")
	// We expect to have date format set to `best_effort`
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"date_time_input_format": "best_effort",
	}))
	insert := fmt.Sprintf("INSERT INTO \"%s\" FORMAT JSONEachRow %s", tableName, insertValues)
	span := lm.phoneHomeAgent.ClickHouseInsertDuration().Begin()
	_, err := lm.chDb.ExecContext(ctx, insert)
	span.End(err)
	if err != nil {
		return end_user_errors.GuessClickhouseErrorType(err).InternalDetails("insert into table '%s' failed", tableName)
	} else {
		return nil
	}
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

func NewEmptyLogManager(cfg config.QuesmaConfiguration, chDb *sql.DB, phoneHomeAgent telemetry.PhoneHomeAgent, loader TableDiscovery, schemaRegistry schema.Registry) *LogManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &LogManager{ctx: ctx, cancel: cancel, chDb: chDb, tableDiscovery: loader, cfg: cfg, phoneHomeAgent: phoneHomeAgent, schemaRegistry: schemaRegistry}
}

func NewLogManager(tables *TableMap, cfg config.QuesmaConfiguration) *LogManager {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(tables)
	return &LogManager{chDb: nil, tableDiscovery: newTableDiscoveryWith(cfg, nil, *tables), cfg: cfg, phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent()}
}

// right now only for tests purposes
func NewLogManagerWithConnection(db *sql.DB, tables *TableMap) *LogManager {
	return &LogManager{chDb: db, tableDiscovery: newTableDiscoveryWith(config.QuesmaConfiguration{}, NewSchemaManagement(db), *tables), phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent()}
}

func NewLogManagerEmpty() *LogManager {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	return &LogManager{tableDiscovery: NewTableDiscovery(config.QuesmaConfiguration{}, nil), phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent()}
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
		hasOthers:                             false,
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
		hasOthers:            false,
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
		hasOthers:            false,
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
		hasOthers:                             false,
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
		hasOthers:            false,
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
		hasOthers:                             false,
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
}

func (c *ChTableConfig) GetAttributes() []Attribute {
	return c.attributes
}
