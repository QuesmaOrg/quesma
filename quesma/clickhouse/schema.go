package clickhouse

import (
	"fmt"
	"math"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/util"
	"reflect"
	"strings"
	"time"
)

type (
	Type interface {
		String() string
		canConvert(interface{}) bool
		createTableString(indentLvl int) string // prints type for CREATE TABLE command
		isArray() bool
		isBool() bool // we need to differentiate between bool and other types. Special method to make it fast
		isString() bool
	}
	Codec struct {
		Name string // change to enum
	}
	BaseType struct {
		Name   string       // ClickHouse name
		goType reflect.Type // can be nil, e.g. for LowCardinality
	}
	CompoundType struct { // only Array for now I think
		Name     string
		BaseType Type
	}
	MultiValueType struct { // Map, Tuple, Nested
		Name string // change to enum?
		Cols []*Column
	}
	Column struct {
		Name      string
		Type      Type
		Modifiers string
		Codec     Codec // TODO currently not used, it's part of Modifiers
	}
	Table struct {
		Name     string
		Database string `default:""`
		Cluster  string `default:""`
		Cols     map[string]*Column
		Config   *ChTableConfig
		Created  bool // do we need to create it during first insert
		indexes  []IndexStatement
	}
)

func (c *Column) String() string {
	return fmt.Sprintf("%s %s", c.Name, c.Type.String())
}

func (t BaseType) String() string {
	return t.Name
}

func (t BaseType) createTableString(indentLvl int) string {
	return t.String()
}

func (t BaseType) isArray() bool { return false }

func (t BaseType) isBool() bool {
	return t.Name == "Bool"
}

func (t BaseType) isString() bool {
	return t.Name == "String"
}

func (t CompoundType) String() string {
	return fmt.Sprintf("%s(%s)", t.Name, t.BaseType.String())
}

func (t CompoundType) createTableString(indentLvl int) string {
	return t.String()
}

func (t CompoundType) isArray() bool { return t.Name == "Array" }

func (t CompoundType) isBool() bool { return false }

func (t CompoundType) isString() bool {
	return false
}

func (t MultiValueType) String() string {
	var sb strings.Builder
	sb.WriteString(t.Name + "(")
	var tupleParams []string
	for _, col := range t.Cols {
		if col != nil {
			// TODO `kibana_sample_data_ecommerce` infers Int64 for those fields as first entries have value `0`
			// 		WORKAROUND: if col.Name == "discount_amount" || col.Name == "unit_discount_amount" -> tupleParams = append(tupleParams, fmt.Sprintf("%s %s", col.Name, "Float64"))
			//	But it's not a good solution, need to find a better one
			tupleParams = append(tupleParams, fmt.Sprintf("%s %s", col.Name, col.Type))
		}
	}
	sb.WriteString(strings.Join(tupleParams, ", "))
	sb.WriteString(")")
	return sb.String()
}

func (t MultiValueType) createTableString(indentLvl int) string {
	var sb strings.Builder
	sb.WriteString(t.Name + "\n" + util.Indent(indentLvl) + "(\n")
	i := 1
	for _, col := range t.Cols {
		sb.WriteString(col.createTableString(indentLvl + 1))
		if i < len(t.Cols) {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
		i++
	}
	sb.WriteString(util.Indent(indentLvl) + ")")
	return sb.String()
}

func (t MultiValueType) isArray() bool { return false }

func (t MultiValueType) isBool() bool { return false }

func (t MultiValueType) isString() bool {
	return false
}

// TODO maybe a bit better/faster?
func (t BaseType) canConvert(v interface{}) bool {
	if t.Name == "String" {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.CanConvert(t.goType) && rv.Equal(rv.Convert(t.goType).Convert(rv.Type()))
}

func (t CompoundType) canConvert(v interface{}) bool {
	return false // TODO for now. For sure can implement arrays easily, maybe some other too
}

func (t MultiValueType) canConvert(v interface{}) bool {
	return false // TODO for now. For sure can implement tuples easily, maybe some other too
}

// 'name' = ClickHouse type name
func NewBaseType(name string) BaseType {
	var goType = reflect.TypeOf("") // default, probably good for dates, etc.
	switch name {
	case "String", "LowCardinality(String)", "UUID":
		goType = reflect.TypeOf("")
	case "DateTime64", "DateTime":
		goType = reflect.TypeOf(time.Time{})
	case "UInt8", "UInt16", "UInt32", "UInt64":
		goType = reflect.TypeOf(uint64(0))
	case "Int8", "Int16", "Int32":
		goType = reflect.TypeOf(int32(0))
	case "Int64":
		goType = reflect.TypeOf(int64(0))
	case "Float32", "Float64":
		goType = reflect.TypeOf(float64(0))
	case "Bool":
		goType = reflect.TypeOf(true)
	case "JSON":
		goType = reflect.TypeOf(map[string]interface{}{})
	}
	return BaseType{Name: name, goType: goType}
}

// 'value': value of a field, from unmarshalled JSON
func NewType(value any) Type {
	isFloatInt := func(f float64) bool {
		return math.Mod(f, 1.0) == 0.0
	}
	switch valueCasted := value.(type) {
	case string:
		t, err := time.Parse(time.RFC3339Nano, valueCasted)
		if err == nil {
			return BaseType{Name: "DateTime64", goType: reflect.TypeOf(t)}
		}
		return BaseType{Name: "String", goType: reflect.TypeOf("")}
	case float64:
		if isFloatInt(valueCasted) {
			return BaseType{Name: "Int64", goType: reflect.TypeOf(int64(0))}
		} else {
			return BaseType{Name: "Float64", goType: reflect.TypeOf(float64(0))}
		}
	case bool:
		return BaseType{Name: "Bool", goType: reflect.TypeOf(true)}
	case map[string]interface{}:
		cols := make([]*Column, len(valueCasted))
		for k, v := range valueCasted {
			cols = append(cols, &Column{Name: k, Type: NewType(v), Codec: Codec{Name: ""}})
		}
		return MultiValueType{Name: "Tuple", Cols: cols}
	case []interface{}:
		if len(valueCasted) == 0 {
			// empty array defaults to string for now, maybe change needed or error returned
			return CompoundType{Name: "Array", BaseType: NewBaseType("String")}
		}
		return CompoundType{Name: "Array", BaseType: NewType(valueCasted[0])}
	}
	return nil // should be unreachable
}

func NewTable(createTableQuery string, config *ChTableConfig) (*Table, error) {
	t, i := ParseCreateTable(createTableQuery)
	t.Config = config
	if i == 0 {
		return t, nil
	} else {
		return t, fmt.Errorf("error parsing query at character %d, query: %s", i, createTableQuery)
	}
}

func (col *Column) isArray() bool {
	return col.Type.isArray()
}

func (col *Column) createTableString(indentLvl int) string {
	spaceStr := " "
	if len(col.Modifiers) == 0 {
		spaceStr = ""
	}
	return util.Indent(indentLvl) + `"` + col.Name + `" ` + col.Type.createTableString(indentLvl) + spaceStr + col.Modifiers
}

func (table *Table) CreateTableString() string {
	dbStr := ""
	if table.Database != "" {
		dbStr = table.Database + "."
	}
	s := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s%s" (
`, dbStr, table.Name)
	rows := make([]string, 0)
	for _, col := range table.Cols {
		rows = append(rows, col.createTableString(1))
	}
	rows = append(rows, table.CreateTableOurFieldsString()...)
	for _, index := range table.indexes {
		rows = append(rows, util.Indent(1)+index.statement())
	}
	return s + strings.Join(rows, ",\n") + "\n)\n" + table.Config.CreateTablePostFieldsString()
}

func (table *Table) CreateTableOurFieldsString() []string {
	rows := make([]string, 0)
	if table.Config.hasOthers {
		_, ok := table.Cols[othersFieldName]
		if !ok {
			rows = append(rows, fmt.Sprintf("%s\"%s\" JSON", util.Indent(1), othersFieldName))
		}
	}
	if table.Config.hasTimestamp {
		_, ok := table.Cols[timestampFieldName]
		if !ok {
			defaultStr := ""
			if table.Config.timestampDefaultsNow {
				defaultStr = " DEFAULT now64()"
			}
			rows = append(rows, fmt.Sprintf("%s\"%s\" DateTime64(3)%s", util.Indent(1), timestampFieldName, defaultStr))
		}
	}
	if len(table.Config.attributes) > 0 {
		for _, a := range table.Config.attributes {
			_, ok := table.Cols[a.KeysArrayName]
			if !ok {
				rows = append(rows, fmt.Sprintf("%s\"%s\" Array(String)", util.Indent(1), a.KeysArrayName))
			}
			_, ok = table.Cols[a.ValuesArrayName]
			if !ok {
				rows = append(rows, fmt.Sprintf("%s\"%s\" Array(%s)", util.Indent(1), a.ValuesArrayName, a.Type.String()))
			}
		}
	}
	return rows
}

func (table *Table) extractColumns(query *model.Query) ([]string, error) {
	N := len(query.Fields)
	if query.IsWildcard() {
		N = len(table.Cols)
	}
	cols := make([]string, 0, N)
	if query.IsWildcard() {
		for _, col := range table.Cols {
			cols = append(cols, col.Name)
		}
	} else {
		for _, field := range query.Fields {
			col, ok := table.Cols[field]
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", field, table.Name)
			}
			cols = append(cols, col.Name)
		}
	}
	return cols, nil
}

// TODO TTL only by timestamp for now!
func (config *ChTableConfig) CreateTablePostFieldsString() string {
	s := "ENGINE = " + config.engine + "\n"
	if config.orderBy != "" {
		s += "ORDER BY " + config.orderBy + "\n"
	}
	if config.partitionBy != "" {
		s += "PARTITION BY " + config.partitionBy + "\n"
	}
	if config.primaryKey != "" {
		s += "PRIMARY KEY " + config.primaryKey + "\n"
	}
	if config.ttl != "" {
		s += "TTL " + config.ttl + "\n"
	}

	if config.settings != "" {
		s += "SETTINGS " + config.settings + "\n"
	}
	return s
}

func NewDefaultStringAttribute() Attribute {
	return Attribute{
		KeysArrayName:   "attributes_string_key",
		ValuesArrayName: "attributes_string_value",
		Type:            NewBaseType("String"),
	}
}

func NewDefaultInt64Attribute() Attribute {
	return Attribute{
		KeysArrayName:   "attributes_int64_key",
		ValuesArrayName: "attributes_int64_value",
		Type:            NewBaseType("Int64"),
	}
}

func NewDefaultFloat64Attribute() Attribute {
	return Attribute{
		KeysArrayName:   "attributes_float64_key",
		ValuesArrayName: "attributes_float64_value",
		Type:            NewBaseType("Float64"),
	}
}

func NewDefaultBoolAttribute() Attribute {
	return Attribute{
		KeysArrayName:   "attributes_bool_key",
		ValuesArrayName: "attributes_bool_value",
		Type:            NewBaseType("Bool"),
	}
}
