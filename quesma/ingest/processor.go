// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"math"
	chLib "quesma/clickhouse"
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
	// IngestProcessor should be renamed to Connector  -> TODO !!!
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
	}
	TableMap  = concurrent.Map[string, *chLib.Table]
	SchemaMap = map[string]interface{} // TODO remove
	Attribute struct {
		KeysArrayName   string
		ValuesArrayName string
		TypesArrayName  string
		MapValueName    string
		MapMetadataName string
		Type            chLib.BaseType
	}
	//chLib.ChTableConfig struct {
	//	hasTimestamp bool // does table have 'timestamp' field
	//	// allow_suspicious_ttl_expressions=1 to enable TTL without date field (doesn't work for me!)
	//	// also be very cautious with it and test it beforehand, people say it doesn't work properly
	//	// TODO make sure it's unique in schema (there's no other 'timestamp' field)
	//	// I (Krzysiek) can write it quickly, but don't want to waste time for it right now.
	//	timestampDefaultsNow bool
	//	engine               string // "Log", "MergeTree", etc.
	//	orderBy              string // "" if none
	//	partitionBy          string // "" if none
	//	primaryKey           string // "" if none
	//	settings             string // "" if none
	//	ttl                  string // of type Interval, e.g. 3 MONTH, 1 YEAR
	//	// look https://clickhouse.com/docs/en/sql-reference/data-types/special-data-types/interval
	//	// "" if none
	//	// TODO make sure it's unique in schema (there's no other 'others' field)
	//	// I (Krzysiek) can write it quickly, but don't want to waste time for it right now.
	//	attributes                            []Attribute
	//	castUnsupportedAttrValueTypesToString bool // if we have e.g. only attrs (String, String), we'll cast e.g. Date to String
	//	preferCastingToOthers                 bool // we'll put non-schema field in [String, String] attrs map instead of others, if we have both options
	//}
)

func NewTableMap() *TableMap {
	return concurrent.NewMap[string, *chLib.Table]()
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

type discoveredTable struct {
	name               string
	columnTypes        map[string]string
	config             config.IndexConfiguration
	comment            string
	createTableQuery   string
	timestampFieldName string
}

func (ip *IngestProcessor) ReloadTables() {
	logger.Info().Msg("reloading tables definitions")
	ip.tableDiscovery.ReloadTableDefinitions()
}

func (ip *IngestProcessor) Close() {
	_ = ip.chDb.Close()
}

// Deprecated: use ResolveIndexes instead, this method will be removed once we switch to the new one
// Indexes can be in a form of wildcard, e.g. "index-*"
// If we have such index, we need to resolve it to a real table name.
func (ip *IngestProcessor) ResolveTableName(index string) (result string) {
	ip.tableDiscovery.TableDefinitions().
		Range(func(k string, v *chLib.Table) bool {
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
func (ip *IngestProcessor) ResolveIndexes(ctx context.Context, patterns string) (results []string, err error) {
	if err = ip.tableDiscovery.TableDefinitionsFetchError(); err != nil {
		return nil, err
	}

	results = make([]string, 0)
	if strings.Contains(patterns, ",") {
		for _, pattern := range strings.Split(patterns, ",") {
			if pattern == elasticsearch.AllIndexesAliasIndexName || pattern == "" {
				results = ip.tableDiscovery.TableDefinitions().Keys()
				slices.Sort(results)
				return results, nil
			} else {
				indexes, err := ip.ResolveIndexes(ctx, pattern)
				if err != nil {
					return nil, err
				}
				results = append(results, indexes...)
			}
		}
	} else {
		if patterns == elasticsearch.AllIndexesAliasIndexName || len(patterns) == 0 {
			results = ip.tableDiscovery.TableDefinitions().Keys()
			slices.Sort(results)
			return results, nil
		} else {
			ip.tableDiscovery.TableDefinitions().
				Range(func(tableName string, v *chLib.Table) bool {
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
func addOurFieldsToCreateTableQuery(q string, config *chLib.ChTableConfig, table *chLib.Table) string {
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
			table.Cols[timestampFieldName] = &chLib.Column{Name: timestampFieldName, Type: chLib.NewBaseType("DateTime64")}
		}
	}
	if len(config.attributes) > 0 {
		for _, a := range config.attributes {
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

func (ip *IngestProcessor) CountMultiple(ctx context.Context, tables ...string) (int64, error) {
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
	err := ip.chDb.QueryRowContext(ctx, fmt.Sprintf("SELECT sum(*) as count FROM (%s)", strings.Join(subCountStatements, " UNION ALL ")), anyTables...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	return count, nil
}

func (ip *IngestProcessor) Count(ctx context.Context, table string) (int64, error) {
	var count int64
	err := ip.chDb.QueryRowContext(ctx, "SELECT count(*) FROM ?", table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	return count, nil
}

func (ip *IngestProcessor) executeRawQuery(query string) (*sql.Rows, error) {
	if res, err := ip.chDb.Query(query); err != nil {
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

func (ip *IngestProcessor) isConnectedToPaidService(service PaidServiceName) (bool, error) {
	rows, err := ip.executeRawQuery(paidServiceChecks[service])
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
func (ip *IngestProcessor) CheckIfConnectedPaidService(service PaidServiceName) (returnedErr error) {
	if _, ok := paidServiceChecks[service]; !ok {
		return fmt.Errorf("service %s is not supported", service)
	}
	totalCheckTime := time.Minute
	startTimeInSeconds := 2.0
	start := time.Now()
	attempt := 0
	for {
		isConnectedToPaidService, err := ip.isConnectedToPaidService(service)
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

func (ip *IngestProcessor) createTableObjectAndAttributes(ctx context.Context, query string, config *chLib.ChTableConfig) (string, error) {
	table, err := chLib.NewTable(query, config)
	if err != nil {
		return "", err
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

func (ip *IngestProcessor) buildCreateTableQueryNoOurFields(ctx context.Context, tableName string,
	jsonData types.JSON, tableConfig *chLib.ChTableConfig, nameFormatter TableColumNameFormatter) ([]CreateTableEntry, map[schema.FieldName]CreateTableEntry) {
	ignoredFields := ip.getIgnoredFields(tableName)

	columnsFromJson := JsonToColumns("", jsonData, 1,
		tableConfig, nameFormatter, ignoredFields)
	columnsFromSchema := SchemaToColumns(findSchemaPointer(ip.schemaRegistry, tableName), nameFormatter)
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
	alteredAttributesIndexes []int) []string {
	var alterCmd []string
	attrKeys := getAttributesByArrayName(chLib.DeprecatedAttributesKeyColumn, attrsMap)
	attrTypes := getAttributesByArrayName(chLib.DeprecatedAttributesValueType, attrsMap)
	var deleteIndexes []int

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
		alterTable := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table.Name, attrKeys[i], columnType)

		newColumns[attrKeys[i]] = &chLib.Column{Name: attrKeys[i], Type: chLib.NewBaseType(attrTypes[i]), Modifiers: modifiers}
		alterCmd = append(alterCmd, alterTable)
		deleteIndexes = append(deleteIndexes, i)
	}

	table.Cols = newColumns

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
) ([]string, types.JSON, []NonSchemaField, error) {

	jsonAsBytesSlice, err := json.Marshal(data)

	if err != nil {
		return nil, nil, nil, err
	}

	// we find all non-schema fields
	jsonMap, err := types.ParseJSON(string(jsonAsBytesSlice))
	if err != nil {
		return nil, nil, nil, err
	}

	if len(config.attributes) == 0 {
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
	if len(config.attributes) <= 0 {
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
		alterCmd = ip.generateNewColumns(attrsMap, table, alteredAttributesIndexes)
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

func fieldToColumnEncoder(field string) string {
	return strings.Replace(field, ".", "::", -1)
}

func (ip *IngestProcessor) processInsertQuery(ctx context.Context,
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
	// Do field encoding here, once for all jsons
	// This is in-place operation
	for _, jsonValue := range jsonData {
		transformFieldName(jsonValue, func(field string) string {
			return fieldToColumnEncoder(field)
		})
	}
	jsonData = processed
	table := ip.FindTable(tableName)
	var tableConfig *chLib.ChTableConfig
	var createTableCmd string
	if table == nil {
		tableConfig = NewOnlySchemaFieldsCHConfig()
		ignoredFields := ip.getIgnoredFields(tableName)
		columnsFromJson := JsonToColumns("", jsonData[0], 1,
			tableConfig, tableFormatter, ignoredFields)
		columnsFromSchema := SchemaToColumns(findSchemaPointer(ip.schemaRegistry, tableName), tableFormatter)
		columns := columnsWithIndexes(columnsToString(columnsFromJson, columnsFromSchema), Indexes(jsonData[0]))
		createTableCmd = createTableQuery(tableName, columns, tableConfig)
		var err error
		createTableCmd, err = ip.createTableObjectAndAttributes(ctx, createTableCmd, tableConfig)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error createTableObjectAndAttributes, can't create table: %v", err)
			return nil, err
		}
		// Set pointer to table after creating it
		table = ip.FindTable(tableName)
	} else if !table.Created {
		createTableCmd = table.createTableString()
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
			invalidJsons[i], tableConfig)
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

func (ip *IngestProcessor) ProcessInsertQuery(ctx context.Context, tableName string,
	jsonData []types.JSON, transformer jsonprocessor.IngestTransformer,
	tableFormatter TableColumNameFormatter) error {
	statements, err := ip.processInsertQuery(ctx, tableName, jsonData, transformer, tableFormatter)
	if err != nil {
		return err
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
	if strings.HasPrefix(query, "ALTER") || strings.HasPrefix(query, "CREATE") {
		logger.InfoWithCtx(ctx).Msgf("DDL query execution: %s", query)
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

func (ip *IngestProcessor) GetTableDefinitions() (TableMap, error) {
	if err := ip.tableDiscovery.TableDefinitionsFetchError(); err != nil {
		return *ip.tableDiscovery.TableDefinitions(), err
	}

	return *ip.tableDiscovery.TableDefinitions(), nil
}

// Returns if schema wasn't created (so it needs to be, and will be in a moment)
func (ip *IngestProcessor) AddTableIfDoesntExist(table *chLib.Table) bool {
	t := ip.FindTable(table.Name)
	if t == nil {
		table.Created = true

		table.applyIndexConfig(ip.cfg)

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

func NewEmptyIngestProcessor(cfg *config.QuesmaConfiguration, chDb *sql.DB, phoneHomeAgent telemetry.PhoneHomeAgent, loader TableDiscovery, schemaRegistry schema.Registry) *IngestProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &IngestProcessor{ctx: ctx, cancel: cancel, chDb: chDb, tableDiscovery: loader, cfg: cfg, phoneHomeAgent: phoneHomeAgent, schemaRegistry: schemaRegistry}
}

func NewIngestProcessor(tables *TableMap, cfg *config.QuesmaConfiguration) *IngestProcessor {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(tables)
	return &IngestProcessor{chDb: nil, tableDiscovery: chLib.NewTableDiscoveryWith(cfg, nil, *tables),
		cfg: cfg, phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(),
		ingestFieldStatistics: make(IngestFieldStatistics)}
}

// right now only for tests purposes
func NewIngestProcessorWithConnection(db *sql.DB, tables *TableMap) *IngestProcessor {
	return &IngestProcessor{chDb: db, tableDiscovery: chLib.NewTableDiscoveryWith(&config.QuesmaConfiguration{}, db, *tables),
		phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(), ingestFieldStatistics: make(IngestFieldStatistics)}
}

func NewIngestProcessorEmpty() *IngestProcessor {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	cfg := &config.QuesmaConfiguration{}
	return &IngestProcessor{tableDiscovery: chLib.NewTableDiscovery(cfg, nil), cfg: cfg,
		phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(), ingestFieldStatistics: make(IngestFieldStatistics)}
}

func NewOnlySchemaFieldsCHConfig() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
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

func NewDefaultCHConfig() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
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

func NewNoTimestampOnlyStringAttrCHConfig() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
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

func NewChTableConfigNoAttrs() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
		hasTimestamp:                          false,
		timestampDefaultsNow:                  false,
		engine:                                "MergeTree",
		orderBy:                               "(" + `"@timestamp"` + ")",
		attributes:                            []Attribute{},
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
}

func NewChTableConfigFourAttrs() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
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

func NewChTableConfigTimestampStringAttr() *chLib.ChTableConfig {
	return &chLib.ChTableConfig{
		hasTimestamp:                          true,
		timestampDefaultsNow:                  true,
		attributes:                            []Attribute{chLib.NewDefaultStringAttribute()},
		engine:                                "MergeTree",
		orderBy:                               "(" + "`@timestamp`" + ")",
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
}

func (c *chLib.ChTableConfig) GetAttributes() []Attribute {
	return c.attributes
}
