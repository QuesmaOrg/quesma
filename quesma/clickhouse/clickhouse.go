package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mailru/go-clickhouse"
	"log"
	"strings"
	"time"
)

const (
	url                = "http://clickhouse:8123"
	timestampFieldName = "timestamp"
	othersFieldName    = "others"
)

type (
	LogManager struct {
		db *sql.DB
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
		Timestamp string `json:"timestamp,omitempty"`
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
	_ = lm.db.Close()
}

func indent(indentLvl int) string {
	return strings.Repeat("\t", indentLvl)
}

func determineFieldType(f interface{}) string {
	if _, ok := f.(bool); ok {
		return "Bool"
	}
	s, ok := f.(string)
	if ok {
		_, err := time.Parse(time.RFC3339Nano, s)
		if err == nil {
			return "DateTime64"
		}
	}
	return "String"
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
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			return fmt.Errorf("open >> %v", err)
		}
		lm.db = connection
	}
	_, err := lm.db.Exec(query)
	if err != nil {
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
		log.Printf("Can't unmarshall, json: %s\nerr:%v\n", jsonData, err)
		return "", err
	}

	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"
(
	%s
)
%s`,
		tableName, FieldsMapToCreateTableString(m, 1, config)+Indexes(m),
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
	if err != nil {
		return fmt.Errorf("can't unmarshall json: %s\nerr:%v", jsonData, err)
	}
	_, err = lm.db.Exec(query)
	if err != nil {
		return fmt.Errorf("error in CreateTable: json: %s\nquery: %s\nerr:%v", PrettyJson(jsonData), query, err)
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
	if len(mDiff) == 0 {         // no need to modify, just insert 'js'
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

func (lm *LogManager) ProcessInsertQuery(tableName, q string) error {
	// first, create table if it doesn't exist
	table := lm.findSchema(tableName) // TODO create tables on start?
	var config *ChTableConfig
	if table == nil {
		config = NewOnlySchemaFieldsCHConfig()
		if strings.Contains(tableName, "_doc") {
			config = NewDefaultCHConfig()
		}
		err := lm.CreateTableFromInsertQuery(tableName, q, config)
		if err != nil {
			fmt.Println("error ProcessInsertQuery:", err)
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
	return lm.Insert(tableName, q, config)
}

func (lm *LogManager) Insert(tableName, jsonData string, config *ChTableConfig) error {
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			fmt.Printf("Open >> %v", err)
		}
		lm.db = connection
	}

	insertJson, err := lm.BuildInsertJson(tableName, jsonData, config)
	if err != nil {
		return err
	}
	insert := fmt.Sprintf("INSERT INTO \"%s\" FORMAT JSONEachRow %s", tableName, insertJson)
	_, err = lm.db.Exec(insert)
	if err != nil {
		return fmt.Errorf("error Insert, tablename: %s\nerror: %v\njson:%s", tableName, err, PrettyJson(jsonData))
	} else {
		log.Printf("Inserted into %s\n", tableName)
		return nil
	}
}

func (lm *LogManager) findSchema(tableName string) *Table {
	v, ok := lm.predefinedTables[tableName]
	if ok {
		return v
	}
	// possible race condition below!! but very unlikely
	return lm.newRuntimeTables[tableName] // check if it returns nil or error
}

// Returns if schema was added
func (lm *LogManager) addSchemaIfDoesntExist(table *Table) bool {
	t := lm.findSchema(table.Name)
	if t == nil {
		table.Created = true
		lm.newRuntimeTables[table.Name] = table // possible race condition
		return true
	}
	return !t.Created
}

func NewLogManager(predefined, newRuntime TableMap) *LogManager {
	db, err := sql.Open("clickhouse", url)
	if err != nil {
		log.Fatal(err)
	}
	return &LogManager{db: db, predefinedTables: predefined, newRuntimeTables: newRuntime}
}

// right now only for tests purposes
func NewLogManagerNoConnection(predefined, newRuntime TableMap) *LogManager {
	return &LogManager{db: nil, predefinedTables: predefined, newRuntimeTables: newRuntime}
}

func NewOnlySchemaFieldsCHConfig() *ChTableConfig {
	return &ChTableConfig{
		hasTimestamp:                          true,
		timestampDefaultsNow:                  true,
		engine:                                "MergeTree",
		orderBy:                               "(timestamp)",
		partitionBy:                           "",
		primaryKey:                            "",
		ttl:                                   "",
		hasOthers:                             false,
		attributes:                            []Attribute{},
		castUnsupportedAttrValueTypesToString: false,
		preferCastingToOthers:                 false,
	}
}

func NewDefaultCHConfig() *ChTableConfig {
	return &ChTableConfig{
		hasTimestamp:         true,
		timestampDefaultsNow: true,
		engine:               "MergeTree",
		orderBy:              "(timestamp)",
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
