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
		predefinedTables map[string]SchemaMap // we don't modify those, safe access
		newRuntimeTables map[string]SchemaMap // potentially unsafe
	}
	SchemaMap = map[string]interface{}
	Log       struct {
		Timestamp string `json:"timestamp,omitempty"`
		Severity  string `json:"severity,omitempty"`
		Message   string `json:"message,omitempty"`
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

func buildCreateTableQuery(tableName, jsonData string, config ChTableConfig) (string, error) {
	m := make(SchemaMap)
	err := json.Unmarshal([]byte(jsonData), &m)
	if err != nil {
		log.Printf("Can't unmarshall, json: %s\nerr:%v\n", jsonData, err)
		return "", err
	}

	if config.hasTimestamp {
		m["timestamp"] = "2024-01-09T15:11:19.299Z" // arbitrary
	}
	orderByStr := ""
	if config.orderBy != "" {
		orderByStr = "ORDER BY " + config.orderBy + "\n"
	}
	partitionByStr := ""
	if config.partitionBy != "" {
		partitionByStr = "PARTITION BY " + config.partitionBy + "\n"
	}
	primaryKeyStr := ""
	if config.primaryKey != "" {
		primaryKeyStr = "PRIMARY KEY " + config.primaryKey + "\n"
	}
	ttlStr := ""
	if config.ttl != "" {
		ttlStr = "TTL toDateTime(timestamp) + INTERVAL " + config.ttl + "\n"
	}
	othersStr := ""
	if config.hasOthers {
		othersStr = indent(1) + "others JSON"
		if len(m) > 0 {
			othersStr += ","
		}
		othersStr += "\n"
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\"\n(\n%s%s)\nENGINE = %s\n%s%s%s%s",
		tableName, othersStr, FieldsMapToCreateTableString(m, 1, config), config.engine,
		orderByStr, partitionByStr, primaryKeyStr, ttlStr), nil
}

func (lm *LogManager) CreateTable(name, jsonData string, config ChTableConfig) error {
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			return fmt.Errorf("open >> %v", err)
		}
		lm.db = connection
	}
	lm.addSchemaIfDoesntExist(name, jsonData)
	query, err := buildCreateTableQuery(name, jsonData, config)
	if err != nil {
		return fmt.Errorf("can't unmarshall json: %s\nerr:%v", jsonData, err)
	}
	_, err = lm.db.Exec(query)
	if err != nil {
		return fmt.Errorf("error in CreateTable: json: %s\nquery: %s\nerr:%v", PrettyJson(jsonData), query, err)
	}
	return nil
}

func (lm *LogManager) BuildInsertJson(tableName, js string, config ChTableConfig) (string, error) {
	if !config.hasOthers {
		return js, nil
	}

	// we create "others" field
	m, err := JsonToFieldsMap(js)
	if err != nil {
		return "", err
	}
	mSchema := lm.findSchema(tableName)
	mDiff := DifferenceMap(mSchema, m)

	others, err := json.Marshal(mDiff)
	if err != nil {
		return "", err
	}

	onlySchemaFields := RemoveNonSchemaFields(mSchema, m)
	schemaFieldsJson, err := json.Marshal(onlySchemaFields)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("{\"%s\":%s,%s", othersFieldName, others, schemaFieldsJson[1:]), nil
}

func (lm *LogManager) Insert(tableName, jsonData string, config ChTableConfig) error {
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

func (lm *LogManager) findSchema(tableName string) SchemaMap {
	v, ok := lm.predefinedTables[tableName]
	if ok {
		return v
	}
	// possible race condition below!! but very unlikely
	return lm.newRuntimeTables[tableName]
}

func (lm *LogManager) addSchemaIfDoesntExist(tableName, jsonInsert string) {
	if lm.findSchema(tableName) == nil {
		m, _ := JsonToFieldsMap(jsonInsert)
		lm.newRuntimeTables[tableName] = m
	}
}

func NewLogManager(predefined, newRuntime map[string]SchemaMap) *LogManager {
	db, err := sql.Open("clickhouse", url)
	if err != nil {
		log.Fatal(err)
	}
	return &LogManager{db: db, predefinedTables: predefined, newRuntimeTables: newRuntime}
}

// right now only for tests purposes
func NewLogManagerNoConnection(predefined, newRuntime map[string]SchemaMap) *LogManager {
	return &LogManager{db: nil, predefinedTables: predefined, newRuntimeTables: newRuntime}
}

func DefaultCHConfig() ChTableConfig {
	return ChTableConfig{
		true,
		true,
		"MergeTree",
		"(timestamp)",
		"",
		"",
		"20 SECOND",
		true,
	}
}

func CustomCHConfig() ChTableConfig {
	return ChTableConfig{
		true,
		true,
		"MergeTree",
		"(timestamp)",
		"",
		"",
		"",
		false,
	}
}
