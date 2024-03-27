package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/index"
	"mitmproxy/quesma/jsonprocessor"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/util"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
)

const (
	timestampFieldName = "@timestamp" // it's always DateTime64 for now, don't want to waste time changing that, we don't seem to use that anyway
	othersFieldName    = "others"
)

type (
	LogManager struct {
		chDb             *sql.DB
		schemaManagement *SchemaManagement
		tableDefinitions *atomic.Pointer[TableMap]
		cfg              config.QuesmaConfiguration
		phoneHomeAgent   telemetry.PhoneHomeAgent
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
		logger.Error().Msgf("could not connect to clickhouse. error: %v", err)
	}

	lm.ReloadTables()
}

func (lm *LogManager) ReloadTables() {
	logger.Info().Msg("reloading tables definitions")
	configuredTables := make(map[string]map[string]string)
	databaseName := "default"
	if lm.cfg.ClickHouseDatabase != "" {
		databaseName = lm.cfg.ClickHouseDatabase
	}
	if tables, err := lm.schemaManagement.readTables(databaseName); err != nil {
		logger.Error().Msgf("could not describe tables: %v", err)
		return
	} else {
		for table, columns := range tables {
			if indexConfig, found := lm.cfg.GetIndexConfig(table); found {
				if indexConfig.Enabled {
					for colName := range columns {
						if _, exists := indexConfig.Aliases[colName]; exists {
							logger.Error().Msgf("column [%s] clashes with an existing alias, table [%s]", colName, table)
						}
					}
					configuredTables[table] = columns
				} else {
					logger.Debug().Msgf("table '%s' is disabled\n", table)
				}
			} else {
				logger.Info().Msgf("table '%s' not configured explicitly\n", table)
			}
		}
	}

	logger.Info().Msgf("discovered tables: [%s]", strings.Join(util.MapKeys(configuredTables), ","))

	populateTableDefinitions(configuredTables, databaseName, lm)
}

func (lm *LogManager) Close() {
	_ = lm.chDb.Close()
}

func (lm *LogManager) matchIndex(indexNamePattern, indexName string) bool {
	r, err := regexp.Compile("^" + strings.Replace(indexNamePattern, "*", ".*", -1) + "$")
	if err != nil {
		logger.Error().Msgf("invalid index name pattern [%s]: %s", indexNamePattern, err)
		return false
	}
	return r.MatchString(indexName)
}

// Deprecated: use ResolveIndexes instead, this method will be removed once we switch to the new one
// Indexes can be in a form of wildcard, e.g. "index-*"
// If we have such index, we need to resolve it to a real table name.
func (lm *LogManager) ResolveTableName(index string) (result string) {
	lm.tableDefinitions.Load().
		Range(func(k string, v *Table) bool {
			if lm.matchIndex(index, k) {
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
func (lm *LogManager) ResolveIndexes(patterns string) (results []string) {
	results = make([]string, 0)
	if strings.Contains(patterns, ",") {
		for _, pattern := range strings.Split(patterns, ",") {
			if pattern == elasticsearch.AllIndexesAliasIndexName || pattern == "" {
				results = lm.tableDefinitions.Load().Keys()
				slices.Sort(results)
				return results
			} else {
				results = append(results, lm.ResolveIndexes(pattern)...)
			}
		}
	} else {
		if patterns == elasticsearch.AllIndexesAliasIndexName || len(patterns) == 0 {
			results = lm.tableDefinitions.Load().Keys()
			slices.Sort(results)
			return results
		} else {
			lm.tableDefinitions.Load().
				Range(func(tableName string, v *Table) bool {
					if lm.matchIndex(patterns, tableName) {
						results = append(results, tableName)
					}
					return true
				})
		}
	}

	slices.Sort(results)
	return slices.Compact(results)
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
		return 0, err
	}
	return count, nil
}

func (lm *LogManager) Count(ctx context.Context, table string) (int64, error) {
	var count int64
	err := lm.chDb.QueryRowContext(ctx, "SELECT count(*) FROM ?", table).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (lm *LogManager) sendCreateTableQuery(ctx context.Context, query string) error {
	if _, err := lm.chDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("error in sendCreateTableQuery: query: %s\nerr:%v", query, err)
	}
	return nil
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

func buildCreateTableQueryNoOurFields(tableName, jsonData string, config *ChTableConfig) (string, error) {
	m := make(SchemaMap)
	err := json.Unmarshal([]byte(jsonData), &m)
	if err != nil {
		logger.Error().Msgf("Can't unmarshall, json: %s\nerr:%v", jsonData, err)
		return "", err
	}
	createTableCmd := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"
(
	%s
)
%s`,
		tableName, FieldsMapToCreateTableString("", m, 1, config)+Indexes(m),
		config.CreateTablePostFieldsString())
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

func (lm *LogManager) CreateTableFromInsertQuery(ctx context.Context, name, jsonData string, config *ChTableConfig) error {
	// TODO fix lm.AddTableIfDoesntExist(name, jsonData)

	query, err := buildCreateTableQueryNoOurFields(name, jsonData, config)
	if err != nil {
		return err
	}

	err = lm.ProcessCreateTableQuery(ctx, query, config)
	if err != nil {
		return err
	}
	return nil
}

func (lm *LogManager) BuildInsertJson(tableName, js string, config *ChTableConfig) (string, error) {
	if !config.hasOthers && len(config.attributes) == 0 {
		return js, nil
	}
	// we find all non-schema fields
	m, err := JsonToFieldsMap(js)
	if err != nil {
		return "", err
	}

	t := lm.GetTable(tableName)
	onlySchemaFields := RemoveTypeMismatchSchemaFields(m, t)
	schemaFieldsJson, err := json.Marshal(onlySchemaFields)

	if err != nil {
		return "", err
	}

	mDiff := DifferenceMap(m, t) // TODO change to DifferenceMap(m, t)

	if len(mDiff) == 0 && string(schemaFieldsJson) == js { // no need to modify, just insert 'js'
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

func (lm *LogManager) GetOrCreateTableConfig(ctx context.Context, tableName, jsonData string) (*ChTableConfig, error) {
	table := lm.GetTable(tableName)
	var config *ChTableConfig
	if table == nil {
		config = NewOnlySchemaFieldsCHConfig()
		err := lm.CreateTableFromInsertQuery(ctx, tableName, jsonData, config)
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

func (lm *LogManager) ProcessInsertQuery(ctx context.Context, tableName string, jsonData []string) error {
	if config, err := lm.GetOrCreateTableConfig(ctx, tableName, jsonData[0]); err != nil {
		return err
	} else {
		return lm.Insert(ctx, tableName, jsonData, config)
	}
}

func (lm *LogManager) Insert(ctx context.Context, tableName string, jsons []string, config *ChTableConfig) error {
	var jsonsReadyForInsertion []string
	for _, jsonValue := range jsons {
		preprocessedJson := preprocess(jsonValue, NestedSeparator)
		insertJson, err := lm.BuildInsertJson(tableName, preprocessedJson, config)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error BuildInsertJson, tablename: %s\nerror: %v\njson:%s", tableName, err, PrettyJson(insertJson))
		}
		jsonsReadyForInsertion = append(jsonsReadyForInsertion, insertJson)
	}

	insertValues := strings.Join(jsonsReadyForInsertion, ", ")

	insert := fmt.Sprintf("INSERT INTO \"%s\" FORMAT JSONEachRow %s", tableName, insertValues)

	span := lm.phoneHomeAgent.ClickHouseInsertDuration().Begin()
	_, err := lm.chDb.ExecContext(ctx, insert)
	span.End(err)
	if err != nil {
		return fmt.Errorf("error on Insert, tablename: [%s]\nerror: [%v]", tableName, err)
	} else {
		return nil
	}
}

func (lm *LogManager) GetTable(tableName string) (result *Table) {
	tableNamePattern := index.TableNamePatternRegexp(tableName)
	lm.tableDefinitions.Load().
		Range(func(name string, table *Table) bool {
			if tableNamePattern.MatchString(name) {
				result = table
				return false
			}
			return true
		})

	return result
}

func (lm *LogManager) GetTableDefinitions() TableMap {
	return *lm.tableDefinitions.Load()
}

// Returns if schema wasn't created (so it needs to be, and will be in a moment)
func (lm *LogManager) AddTableIfDoesntExist(table *Table) bool {
	t := lm.GetTable(table.Name)
	if t == nil {
		table.Created = true

		table.applyIndexConfig(lm.cfg)

		lm.tableDefinitions.Load().Store(table.Name, table)
		return true
	}
	wasntCreated := !t.Created
	t.Created = true
	return wasntCreated
}

func NewEmptyLogManager(cfg config.QuesmaConfiguration, chDb *sql.DB, phoneHomeAgent telemetry.PhoneHomeAgent) *LogManager {
	var schemaManagement = NewSchemaManagement(chDb)
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	return &LogManager{chDb: chDb, tableDefinitions: &tableDefinitions, cfg: cfg, schemaManagement: schemaManagement, phoneHomeAgent: phoneHomeAgent}
}

func NewLogManager(tables *TableMap, cfg config.QuesmaConfiguration) *LogManager {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(tables)
	return &LogManager{chDb: nil, tableDefinitions: &tableDefinitions, cfg: cfg, phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent()}
}

// right now only for tests purposes
func NewLogManagerWithConnection(db *sql.DB, tables *TableMap) *LogManager {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(tables)
	return &LogManager{chDb: db, tableDefinitions: &tableDefinitions, schemaManagement: NewSchemaManagement(db), phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent()}
}

func NewLogManagerEmpty() *LogManager {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	return &LogManager{tableDefinitions: &tableDefinitions, phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent()}
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

func preprocess(jsonStr string, nestedSeparator string) string {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(jsonStr), &data)

	resultJSON, _ := json.Marshal(jsonprocessor.FlattenMap(data, nestedSeparator))
	return string(resultJSON)
}
