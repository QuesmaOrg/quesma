package clickhouse

import (
	"fmt"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/util"
	"strconv"
	"strings"
)

type Table struct {
	Name         string
	DatabaseName string `default:""`
	Cluster      string `default:""`
	Cols         map[string]*Column
	Config       *ChTableConfig
	Created      bool // do we need to create it during first insert
	indexes      []IndexStatement
}

func (t *Table) createTableOurFieldsString() []string {
	rows := make([]string, 0)
	if t.Config.hasOthers {
		_, ok := t.Cols[othersFieldName]
		if !ok {
			rows = append(rows, fmt.Sprintf("%s\"%s\" JSON", util.Indent(1), othersFieldName))
		}
	}
	if t.Config.hasTimestamp {
		_, ok := t.Cols[timestampFieldName]
		if !ok {
			defaultStr := ""
			if t.Config.timestampDefaultsNow {
				defaultStr = " DEFAULT now64()"
			}
			rows = append(rows, fmt.Sprintf("%s\"%s\" DateTime64(3)%s", util.Indent(1), timestampFieldName, defaultStr))
		}
	}
	if len(t.Config.attributes) > 0 {
		for _, a := range t.Config.attributes {
			_, ok := t.Cols[a.KeysArrayName]
			if !ok {
				rows = append(rows, fmt.Sprintf("%s\"%s\" Array(String)", util.Indent(1), a.KeysArrayName))
			}
			_, ok = t.Cols[a.ValuesArrayName]
			if !ok {
				rows = append(rows, fmt.Sprintf("%s\"%s\" Array(%s)", util.Indent(1), a.ValuesArrayName, a.Type.String()))
			}
		}
	}
	return rows
}

func (t *Table) extractColumns(query *model.Query, addNonSchemaFields bool) ([]string, error) {
	N := len(query.Fields)
	if query.IsWildcard() {
		N = len(t.Cols)
	}
	cols := make([]string, 0, N)
	if query.IsWildcard() {
		for _, col := range t.Cols {
			cols = append(cols, col.Name)
		}
	} else {
		for _, field := range query.Fields {
			if field == model.EmptyFieldSelection {
				cols = append(cols, "")
				continue
			}
			col, ok := t.Cols[field]
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", field, t.Name)
			}
			cols = append(cols, col.Name)
		}
		if addNonSchemaFields {
			for _, field := range query.NonSchemaFields {
				if strings.Contains(field, "AS") {
					components := strings.Split(field, " AS ")
					fieldNameMaybeQuoted := strings.TrimSpace(components[1])
					cols = append(cols, strings.Trim(fieldNameMaybeQuoted, "`"))
				} else {
					cols = append(cols, field)
				}
			}
		}
	}
	return cols, nil
}

func (t *Table) createTableString() string {
	s := "CREATE TABLE IF NOT EXISTS " + t.FullTableName() + " (\n"
	rows := make([]string, 0)
	for _, col := range t.Cols {
		rows = append(rows, col.createTableString(1))
	}
	rows = append(rows, t.createTableOurFieldsString()...)
	for _, index := range t.indexes {
		rows = append(rows, util.Indent(1)+index.statement())
	}
	return s + strings.Join(rows, ",\n") + "\n)\n" + t.Config.CreateTablePostFieldsString()
}

// FullTableName returns full table name with database name if it's not empty.
// in a format: ["database".]"table" as it seems to work for all cases in Clickhouse.
// You can use it in any query to Clickhouse, e.g. in FROM ... clause.
func (t *Table) FullTableName() string {
	if t.DatabaseName != "" {
		return strconv.Quote(t.DatabaseName) + "." + strconv.Quote(t.Name)
	} else {
		return strconv.Quote(t.Name)
	}
}

// GetDateTimeType returns type of a field (currently DateTime/DateTime64), if it's a DateTime type. Invalid otherwise.
// Timestamp from config defaults to DateTime64.
func (t *Table) GetDateTimeType(fieldName string) DateTimeType {
	if col, ok := t.Cols[fieldName]; ok {
		typeName := col.Type.String()
		// hasPrefix, not equal, because we can have DateTime64(3) and we want to catch it
		if strings.HasPrefix(typeName, "DateTime64") {
			return DateTime64
		}
		if strings.HasPrefix(typeName, "DateTime") {
			return DateTime
		}
	}
	if t.Config.hasTimestamp && fieldName == timestampFieldName {
		return DateTime64
	}
	return Invalid
}

// applyFullTextSearchConfig applies full text search configuration to the table
func (t *Table) applyFullTextSearchConfig(configuration config.QuesmaConfiguration) {
	for _, c := range t.Cols {
		c.IsFullTextMatch = configuration.IsFullTextMatchField(t.Name, c.Name)
	}
}

func (t *Table) GetAttributesList() []Attribute {
	return t.Config.attributes
}

// TODO Won't work with tuples, e.g. trying to access via tupleName.tupleField will return NotExists,
// instead of some other response. Fix this when needed (we seem to not need tuples right now)
func (t *Table) GetFieldInfo(fieldName string) FieldInfo {
	col, ok := t.Cols[fieldName]
	if !ok {
		return NotExists
	}
	if col.isArray() {
		return ExistsAndIsArray
	}
	return ExistsAndIsBaseType
}
