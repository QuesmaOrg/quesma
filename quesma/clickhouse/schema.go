package clickhouse

import (
	"fmt"
	"math"
	"reflect"
	"strings"
)

type (
	Type interface {
		String() string
		canConvert(interface{}) bool
	}
	Codec struct {
		Name string // change to enum
	}
	BaseType struct {
		Name   string       // ClickHouse name
		goType reflect.Type // can be nil, e.g. for LowCardinality
	}
	CompoundType struct { // Array, LowCardinality TODO LowCardinality should not be here
		Name     string
		BaseType Type
	}
	MultiValueType struct { // Map, Tuple, Nested
		Name string // change to enum?
		Cols []*Column
	}
	Column struct {
		Name  string
		Type  Type
		Codec Codec // maybe not needed? idk now
	}
	Table struct {
		Name     string
		Database string `default:""`
		Cluster  string `default:""`
		Cols     map[string]*Column
		Config   *ChTableConfig
		Created  bool // do we need to create it during first insert
	}
)

func (t BaseType) String() string {
	return t.Name
}

func (t CompoundType) String() string {
	return t.Name + "(" + t.BaseType.String() + ")"
}

func (t MultiValueType) String() string {
	var sb strings.Builder
	sb.WriteString(t.Name + "(")
	for i, col := range t.Cols {
		sb.WriteString(col.Name)
		if i+1 < len(t.Cols) {
			sb.WriteString(",")
		}
	}
	sb.WriteString(")")
	return sb.String()
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
	var goType reflect.Type = nil
	switch name {
	case "String":
		goType = reflect.TypeOf("")
	case "UInt8", "UInt16", "UInt32", "UInt64":
		goType = reflect.TypeOf(uint64(0))
	case "Int8", "Int16", "Int32", "Int64":
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
func NewType(value interface{}) Type {
	isFloatInt := func(f float64) bool {
		return math.Mod(f, 1.0) == 0.0
	}
	switch valueCasted := value.(type) {
	case string:
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
