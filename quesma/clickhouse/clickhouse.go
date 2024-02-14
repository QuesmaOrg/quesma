package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mailru/go-clickhouse"
	"mitmproxy/quesma/index"
	"mitmproxy/quesma/jsonprocessor"
	"mitmproxy/quesma/logger"
	"net/url"
	"regexp"
	"strings"
)

const (
	timestampFieldName = "@timestamp"
	othersFieldName    = "others"
)

type (
	LogManager struct {
		chUrl *url.URL
		chDb  *sql.DB
		// I split schemas into 2. My motivation is that 'newRuntimeTables' are modified
		// during runtime. It's very unlikely, but (AFAIK) race condition may happen, as we have no
		// synchronization mechanisms. If we know schemas beforehand, we can put them in 'predefinedTables',
		// which we never modify, so it's safe to access it from multiple goroutines.
		predefinedTables TableMap // we don't modify those, safe access
		newRuntimeTables TableMap // potentially unsafe
	}
	TableMap  = map[string]*Table
	SchemaMap = map[string]interface{} // TODO remnove
	Log       struct {
		Timestamp string `json:"@timestamp,omitempty"`
		Severity  string `json:"severity,omitempty"`
		Message   string `json:"message,omitempty"`
	}
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

func (lm *LogManager) Close() {
	_ = lm.chDb.Close()
}

func (lm *LogManager) initConnection() error {
	if lm.chDb == nil {
		connection, err := sql.Open("clickhouse", lm.chUrl.String())
		if err != nil {
			return fmt.Errorf("open >> %v", err)
		}
		lm.chDb = connection
	}
	return nil
}

func (lm *LogManager) matchIndex(indexNamePattern, indexName string) bool {
	r, err := regexp.Compile("^" + strings.Replace(indexNamePattern, "*", ".*", -1) + "$")
	if err != nil {
		logger.Error().Msgf("invalid index name pattern [%s]: %s", indexNamePattern, err)
		return false
	}
	return r.MatchString(indexName)
}

// Indexes can be in a form of wildcard, e.g. "index-*"
// If we have such index, we need to resolve it to a real table name.
func (lm *LogManager) ResolveTableName(index string) string {
	for k := range lm.predefinedTables {
		if lm.matchIndex(index, k) {
			return k
		}
	}
	for k := range lm.newRuntimeTables {
		if lm.matchIndex(index, k) {
			return k
		}
	}
	return ""
}

func (lm *LogManager) findSchemaAndInitConnection(tableName string) (*Table, error) {
	table := lm.findSchema(tableName)
	if table == nil {
		return nil, fmt.Errorf("table matching [%s] not found", tableName)
	}
	if err := lm.initConnection(); err != nil {
		return nil, err
	}
	return table, nil
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
			othersStr = fmt.Sprintf("%s\"%s\" JSON,\n", indent(1), othersFieldName)
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
			timestampStr = fmt.Sprintf("%s\"%s\" DateTime64(3)%s,\n", indent(1), timestampFieldName, defaultStr)
			table.Cols[timestampFieldName] = &Column{Name: timestampFieldName, Type: NewBaseType("DateTime64")}
		}
	}
	if len(config.attributes) > 0 {
		for _, a := range config.attributes {
			_, ok := table.Cols[a.KeysArrayName]
			if !ok {
				attributesStr += fmt.Sprintf("%s\"%s\" Array(String),\n", indent(1), a.KeysArrayName)
				table.Cols[a.KeysArrayName] = &Column{Name: a.KeysArrayName, Type: CompoundType{Name: "Array", BaseType: NewBaseType("String")}}
			}
			_, ok = table.Cols[a.ValuesArrayName]
			if !ok {
				attributesStr += fmt.Sprintf("%s\"%s\" Array(%s),\n", indent(1), a.ValuesArrayName, a.Type.String())
				table.Cols[a.ValuesArrayName] = &Column{Name: a.ValuesArrayName, Type: a.Type}
			}
		}
	}

	i := strings.Index(q, "(")
	return q[:i+2] + othersStr + timestampStr + attributesStr + q[i+1:]
}

func (lm *LogManager) sendCreateTableQuery(query string) error {
	if lm.chDb == nil {
		connection, err := sql.Open("clickhouse", lm.chUrl.String())
		if err != nil {
			return fmt.Errorf("open >> %v", err)
		}
		lm.chDb = connection
	}
	if _, err := lm.chDb.Exec(query); err != nil {
		return fmt.Errorf("error in sendCreateTableQuery: query: %s\nerr:%v", query, err)
	}
	return nil
}

func (lm *LogManager) ProcessCreateTableQuery(query string, config *ChTableConfig) error {
	table, err := NewTable(query, config)
	if err != nil {
		return err
	}

	// if exists only then createTable
	noSuchTable := lm.addSchemaIfDoesntExist(table)
	if !noSuchTable {
		return fmt.Errorf("table %s already exists", table.Name)
	}

	return lm.sendCreateTableQuery(addOurFieldsToCreateTableQuery(query, config, table))
}

func buildCreateTableQueryNoOurFields(tableName, jsonData string, config *ChTableConfig) (string, error) {
	m := make(SchemaMap)
	err := json.Unmarshal([]byte(jsonData), &m)
	if err != nil {
		logger.Error().Msgf("Can't unmarshall, json: %s\nerr:%v", jsonData, err)
		return "", err
	}

	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"
(
	%s
)
%s`,
		tableName, FieldsMapToCreateTableString("", m, 1, config)+Indexes(m),
		config.CreateTablePostFieldsString()), nil
}

func Indexes(m SchemaMap) string {
	var result strings.Builder
	for col := range m {
		index := getIndexStatement(col)
		if index != "" {
			result.WriteString(",\n")
			result.WriteString(indent(1))
			result.WriteString(index.statement())
		}
	}
	result.WriteString(",\n")
	return result.String()
}

func (lm *LogManager) CreateTableFromInsertQuery(name, jsonData string, config *ChTableConfig) error {
	// TODO fix lm.addSchemaIfDoesntExist(name, jsonData)

	query, err := buildCreateTableQueryNoOurFields(name, jsonData, config)
	if err != nil {
		return err
	}

	err = lm.ProcessCreateTableQuery(query, config)
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

	t := lm.findSchema(tableName)
	mDiff := DifferenceMap(m, t) // TODO change to DifferenceMap(m, t)

	if len(mDiff) == 0 { // no need to modify, just insert 'js'
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
	onlySchemaFields := RemoveNonSchemaFields(m, t)
	schemaFieldsJson, err := json.Marshal(onlySchemaFields)
	if err != nil {
		return "", err
	}
	comma := ""
	if nonSchemaStr != "" && len(schemaFieldsJson) > 2 {
		comma = "," // need to watch out where we input commas, CH doesn't tolerate trailing ones
	}
	return fmt.Sprintf("{%s%s%s", nonSchemaStr, comma, schemaFieldsJson[1:]), nil
}

func (lm *LogManager) ProcessInsertQuery(tableName, jsonData string) error {
	// first, create table if it doesn't exist
	table := lm.findSchema(tableName) // TODO create tables on start?
	var config *ChTableConfig
	if table == nil {
		config = NewOnlySchemaFieldsCHConfig()
		err := lm.CreateTableFromInsertQuery(tableName, jsonData, config)
		if err != nil {
			logger.Error().Msgf("error ProcessInsertQuery, can't create table: %v", err)
			return err
		}
	} else if !table.Created {
		err := lm.sendCreateTableQuery(table.CreateTableString())
		if err != nil {
			return err
		}
		config = table.Config
	} else {
		config = table.Config
	}

	// then insert
	return lm.Insert(tableName, jsonData, config)
}

func (lm *LogManager) Insert(tableName, jsonData string, config *ChTableConfig) error {
	if lm.chDb == nil {
		connection, err := sql.Open("clickhouse", lm.chUrl.String())
		if err != nil {
			logger.Error().Msgf("Open >> %v", err)
		}
		lm.chDb = connection
	}

	processed := preprocess(jsonData, NestedSeparator)
	insertJson, err := lm.BuildInsertJson(tableName, processed, config)
	if err != nil {
		return err
	}
	insert := fmt.Sprintf("INSERT INTO \"%s\" FORMAT JSONEachRow %s", tableName, insertJson)
	_, err = lm.chDb.Exec(insert)
	if err != nil {
		return fmt.Errorf("error Insert, tablename: %s\nerror: %v\njson:%s", tableName, err, PrettyJson(jsonData))
	} else {
		return nil
	}
}

func (lm *LogManager) findSchema(tableName string) *Table {
	tableNamePattern := index.TableNamePatternRegexp(tableName)
	for name, table := range lm.predefinedTables {
		if tableNamePattern.MatchString(name) {
			return table
		}
	}
	for name, table := range lm.newRuntimeTables {
		if tableNamePattern.MatchString(name) {
			return table
		}
	}

	v, ok := lm.predefinedTables[tableName]
	if ok {
		return v
	}
	// possible race condition below!! but very unlikely
	return lm.newRuntimeTables[tableName] // check if it returns nil or error
}

// Returns if schema wasn't created (so it needs to be, and will be in a moment)
func (lm *LogManager) addSchemaIfDoesntExist(table *Table) bool {
	t := lm.findSchema(table.Name)
	if t == nil {
		table.Created = true
		lm.newRuntimeTables[table.Name] = table // possible race condition
		return true
	}
	wasntCreated := !t.Created
	t.Created = true
	return wasntCreated
}

func NewLogManager(dbUrl *url.URL, predefined, newRuntime TableMap) *LogManager {
	db, err := sql.Open("clickhouse", dbUrl.String())
	if err != nil {
		logger.Fatal().Msg(err.Error())
	}
	return &LogManager{chUrl: dbUrl, chDb: db, predefinedTables: predefined, newRuntimeTables: newRuntime}
}

// right now only for tests purposes
func NewLogManagerWithConnection(db *sql.DB, predefined, newRuntime TableMap) *LogManager {
	return &LogManager{chDb: db, predefinedTables: predefined, newRuntimeTables: newRuntime}
}

func NewLogManagerEmpty() *LogManager {
	return &LogManager{predefinedTables: make(TableMap), newRuntimeTables: make(TableMap)}
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

func preprocess(jsonStr string, nestedSeparator string) string {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(jsonStr), &data)

	resultJSON, _ := json.Marshal(jsonprocessor.FlattenMap(data, nestedSeparator))
	return string(resultJSON)
}
