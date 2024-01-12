package clickhouse

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mailru/go-clickhouse"
	"log"
	"strings"
)

const url = "http://clickhouse:8123"

type (
	LogManager struct {
		db *sql.DB
	}
	Log struct {
		Timestamp string `json:"timestamp,omitempty"`
		Severity  string `json:"severity,omitempty"`
		Message   string `json:"message,omitempty"`
	}
)

func (lm *LogManager) Close() {
	_ = lm.db.Close()
}

func indent(indentLvl int) string {
	return strings.Repeat("\t", indentLvl)
}

// m: unmarshalled json from HTTP request
// Returns nicely formatted string for CREATE TABLE command
func parseNestedJson(m map[string]interface{}, indentLvl int) string {
	var result strings.Builder
	i := 0
	for name, value := range m {
		result.WriteString(indent(indentLvl))
		nestedValue, ok := value.(map[string]interface{})
		if ok { // value is another (nested) dict
			// quotes near field names very important. Normally they are not, but
			// they enable to have fields with reserved names, like e.g. index.
			result.WriteString(fmt.Sprintf("\"%s\" Tuple\n%s(\n%s%s)", name,
				indent(indentLvl), parseNestedJson(nestedValue, indentLvl+1), indent(indentLvl)))
		} else {
			// value is a single field. Only String/Bool supported for now.
			fType := "String"
			_, ok := m[name].(bool)
			if ok {
				fType = "Bool"
			}
			result.WriteString(fmt.Sprintf("\"%s\" %s", name, fType))
		}
		if i+1 < len(m) {
			result.WriteString(",")
		}
		i++
		result.WriteString("\n")
	}
	return result.String()
}

func PrettyJson(jsonStr string) string {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, []byte(jsonStr), "", "    "); err != nil {
		return fmt.Sprintf("PrettyJson err: %v\n", err)
	}
	return prettyJSON.String()
}

func (lm *LogManager) CreateTable(name, jsonData string) error {
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			return fmt.Errorf("open >> %v", err)
		}
		lm.db = connection
	}

	m := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonData), &m)
	if err != nil {
		return fmt.Errorf("can't unmarshall, json: %s err:%v", jsonData, err)
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\"\n(\n%s)\nENGINE = Log\n",
		name, parseNestedJson(m, 1))
	_, err = lm.db.Exec(query)
	if err != nil {
		return fmt.Errorf("json: %s\nquery: %s\nerr:%v", PrettyJson(jsonData), query, err)
	}
	return nil
}

func (lm *LogManager) Insert(tableName, jsonData string) error {
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			fmt.Printf("Open >> %v", err)
		}
		lm.db = connection
	}

	insert := fmt.Sprintf("INSERT INTO \"%s\" FORMAT JSONEachRow %s", tableName, jsonData)
	_, err := lm.db.Exec(insert)
	if err != nil {
		return fmt.Errorf("tablename: %s, error: %v\njson:%s", tableName, err, PrettyJson(jsonData))
	} else {
		log.Printf("Inserted into %s\n", tableName)
		return nil
	}
}

func NewLogManager() *LogManager {
	db, err := sql.Open("clickhouse", url)
	if err != nil {
		log.Fatal(err)
	}
	return &LogManager{db: db}
}
