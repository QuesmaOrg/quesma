// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/k0kubun/pp"
	"math"
	"reflect"
	"strings"
	"time"
)

const (
	// FIXME: Remnants of old way of storing attributes
	DeprecatedAttributesKeyColumn   = "attributes_string_key"
	DeprecatedAttributesValueColumn = "attributes_string_value"
	DeprecatedAttributesValueType   = "attributes_string_type"

	attributesColumnType     = "Map(String, String)" // ClickHouse type of AttributesValuesColumn, AttributesMetadataColumn
	AttributesValuesColumn   = "attributes_values"
	AttributesMetadataColumn = "attributes_metadata"

	UndefinedType = "Undefined" // used for unknown types or incomplete types for which NewType can't infer a proper type
)

type (
	Type interface {
		String() string
		StringWithNullable() string // just like String but displays also 'Nullable' if it's nullable
		CanConvert(interface{}) bool
		createTableString(indentLvl int) string // prints type for CREATE TABLE command
		isArray() bool
		isBool() bool // we need to differentiate between bool and other types. Special method to make it fast
		isString() bool
		IsNullable() bool
	}
	Codec struct {
		Name string // change to enum
	}
	BaseType struct {
		Name     string       // ClickHouse name
		GoType   reflect.Type // can be nil, e.g. for LowCardinality
		Nullable bool         // if it's Nullable
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
		Comment   string
	}
	DateTimeType int
)

const (
	DateTime64 DateTimeType = iota
	DateTime
	Invalid
)

func (c *Column) String() string {
	return fmt.Sprintf("%s %s", c.Name, c.Type.String())
}

func (t BaseType) String() string {
	return t.Name
}

func (t BaseType) StringWithNullable() string {
	if t.Nullable {
		return "Nullable(" + t.Name + ")"
	}
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

func (t BaseType) IsNullable() bool { return t.Nullable }

func (t CompoundType) String() string {
	return fmt.Sprintf("%s(%s)", t.Name, t.BaseType.String())
}

// StringWithNullable is the same as String(), as compound types can't be nullable in Clickhouse
func (t CompoundType) StringWithNullable() string { return t.String() }

func (t CompoundType) createTableString(indentLvl int) string {
	return t.String()
}

func (t CompoundType) isArray() bool { return t.Name == "Array" }

func (t CompoundType) isBool() bool { return false }

func (t CompoundType) isString() bool {
	return false
}

func (t CompoundType) IsNullable() bool { return false }

func (t MultiValueType) String() string {
	var sb strings.Builder
	sb.WriteString(t.Name + "(")
	var tupleParams []string
	for _, col := range t.Cols {
		if col != nil {
			// TODO `kibana_sample_data_ecommerce` infers Int64 for those fields as first entries have value `0`
			// 		WORKAROUND: if col.Name == "discount_amount" || col.Name == "unit_discount_amount" -> tupleParams = append(tupleParams, fmt.Sprintf("%s %s", col.Name, "Float64"))
			//	But it's not a good solution, need to find a better one
			colType := col.Type.String()
			if (!strings.Contains(colType, "Array") && !strings.Contains(colType, "Tuple")) && !strings.Contains(colType, "DateTime") {
				colType = "Nullable(" + colType + ")"
			}
			tupleParams = append(tupleParams, fmt.Sprintf("%s %s", col.Name, colType))
		}
	}
	sb.WriteString(strings.Join(tupleParams, ", "))
	sb.WriteString(")")
	return sb.String()
}

// StringWithNullable is the same as String(), as not-base types can't be nullable in Clickhouse
func (t MultiValueType) StringWithNullable() string {
	return t.String()
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

func (t MultiValueType) IsNullable() bool {
	return false
}

// TODO maybe a bit better/faster?
func (t BaseType) CanConvert(v interface{}) bool {
	if t.Name == "String" {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.CanConvert(t.GoType) && rv.Equal(rv.Convert(t.GoType).Convert(rv.Type()))
}

func (t CompoundType) CanConvert(v interface{}) bool {
	return false // TODO for now. For sure can implement arrays easily, maybe some other too
}

func (t MultiValueType) CanConvert(v interface{}) bool {
	return false // TODO for now. For sure can implement tuples easily, maybe some other too
}

func (t MultiValueType) GetColumn(name string) *Column {
	// TODO: linear scan, but this will suffice for now (Tuples aren't typically large)
	for _, col := range t.Cols {
		if col.Name == name {
			return col
		}
	}
	return nil
}

func NewBaseType(clickHouseTypeName string) BaseType {
	var GoType = ResolveType(clickHouseTypeName)
	if GoType == nil {
		// default, probably good for dates, etc.
		GoType = reflect.TypeOf("")
	}
	return BaseType{Name: clickHouseTypeName, GoType: GoType}
}

// this is catch all type for all types we do not exlicitly support
type UnknownType struct{}

func ResolveType(clickHouseTypeName string) reflect.Type {
	switch clickHouseTypeName {
	case "String", "LowCardinality(String)", "UUID", "FixedString":
		return reflect.TypeOf("")
	case "DateTime64", "DateTime", "Date", "DateTime64(3)":
		return reflect.TypeOf(time.Time{})
	case "UInt8", "UInt16", "UInt32", "UInt64":
		return reflect.TypeOf(uint64(0))
	case "Int8", "Int16", "Int32":
		return reflect.TypeOf(int32(0))
	case "Int64":
		return reflect.TypeOf(int64(0))
	case "Float32", "Float64":
		return reflect.TypeOf(float64(0))
	case "Point":
		return reflect.TypeOf(Point{})
	case "Bool":
		return reflect.TypeOf(true)
	case "JSON":
		return reflect.TypeOf(map[string]interface{}{})
	case "Map(String, Nullable(String))", "Map(String, String)":
		return reflect.TypeOf(map[string]string{})
	case "Unknown":
		return reflect.TypeOf(UnknownType{})
	}

	return nil
}

// 'value': value of a field, from unmarshalled JSON
// 'valueOrigin': name of the field (for error messages)
func NewType(value any, valueOrigin string) (Type, error) {
	isFloatInt := func(f float64) bool {
		return math.Mod(f, 1.0) == 0.0
	}
	switch valueCasted := value.(type) {
	case string:
		t, err := time.Parse(time.RFC3339Nano, valueCasted)
		if err == nil {
			return BaseType{Name: "DateTime64", GoType: reflect.TypeOf(t)}, nil
		}
		t, err = time.Parse("2006-01-02T15:04:05", valueCasted)
		if err == nil {
			return BaseType{Name: "DateTime64", GoType: reflect.TypeOf(t)}, nil
		}
		return BaseType{Name: "String", GoType: reflect.TypeOf("")}, nil
	case float64:
		if isFloatInt(valueCasted) {
			return BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0))}, nil
		} else {
			return BaseType{Name: "Float64", GoType: reflect.TypeOf(float64(0))}, nil
		}
	case int:
		return BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0))}, nil
	case bool:
		return BaseType{Name: "Bool", GoType: reflect.TypeOf(true)}, nil
	case map[string]interface{}:
		cols := make([]*Column, len(valueCasted))
		for k, v := range valueCasted {
			innerName := fmt.Sprintf("%s.%s", valueOrigin, k)
			innerType, err := NewType(v, innerName)
			if err != nil {
				return nil, err
			}
			cols = append(cols, &Column{Name: k, Type: innerType, Codec: Codec{Name: ""}})
		}
		if len(cols) == 0 {
			logger.DeduplicatedWarn().Msgf("Empty map type (origin: %s).", valueOrigin)
			return nil, fmt.Errorf("empty map type (origin: %s)", valueOrigin)
		}
		return MultiValueType{Name: "Tuple", Cols: cols}, nil
	case []interface{}:
		if len(valueCasted) == 0 {
			logger.DeduplicatedWarn().Msgf("Empty array type (origin: %s).", valueOrigin)
			return nil, fmt.Errorf("empty array type (origin: %s)", valueOrigin)
		}
		innerName := fmt.Sprintf("%s[0]", valueOrigin)
		innerType, err := NewType(valueCasted[0], innerName)
		if err != nil {
			return nil, err
		}
		return CompoundType{Name: "Array", BaseType: innerType}, nil
	case nil:
		logger.DeduplicatedWarn().Msgf("Nil type (origin: %s).", valueOrigin)
		return nil, fmt.Errorf("nil type (origin: %s)", valueOrigin)
	}

	logger.DeduplicatedWarn().Msgf("Unsupported type '%T' of value: %v (origin: %s).", value, value, valueOrigin)
	return nil, fmt.Errorf("unsupported type '%T' of value: %v (origin: %s)", value, value, valueOrigin)
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

// NewEmptyTable is used only in tests
func NewEmptyTable(tableName string) *Table {
	return &Table{Name: tableName, Config: NewChTableConfigNoAttrs()}
}

func (col *Column) isArray() bool {
	return col.Type.isArray()
}

func (col *Column) createTableString(indentLvl int) string {
	maybeSpace := " "
	if len(col.Modifiers) == 0 {
		maybeSpace = ""
	}
	comment := fmt.Sprintf(" COMMENT '%s'", col.Comment)
	if len(col.Comment) == 0 {
		comment = ""
	}
	return fmt.Sprintf(`%s"%s" %s%s%s%s`, util.Indent(indentLvl), col.Name, col.Type.createTableString(indentLvl),
		maybeSpace, col.Modifiers, comment)
}

// TODO TTL only by timestamp for now!
func (config *ChTableConfig) CreateTablePostFieldsString() string {
	pp.Println("aaaa", config)
	s := "ENGINE = " + config.Engine + "\n"
	if config.OrderBy != "" {
		s += "ORDER BY " + config.OrderBy + "\n"
	}
	if config.PartitionBy != "" {
		s += "PARTITION BY " + config.PartitionBy + "\n"
	}
	if config.PrimaryKey != "" {
		s += "PRIMARY KEY " + config.PrimaryKey + "\n"
	}
	if config.Ttl != "" {
		s += "TTL " + config.Ttl + "\n"
	}

	if config.Settings != "" {
		s += "SETTINGS " + config.Settings + "\n"
	}
	return s
}

func NewDefaultStringAttribute() Attribute {
	return Attribute{
		KeysArrayName:   DeprecatedAttributesKeyColumn,
		ValuesArrayName: DeprecatedAttributesValueColumn,
		TypesArrayName:  DeprecatedAttributesValueType,
		MapValueName:    AttributesValuesColumn,
		MapMetadataName: AttributesMetadataColumn,
		Type:            NewBaseType("String"),
	}
}

func NewDefaultInt64Attribute() Attribute {
	return Attribute{
		KeysArrayName:   "attributes_int64_key",
		ValuesArrayName: "attributes_int64_value",
		TypesArrayName:  "attributes_int64_type",
		MapValueName:    AttributesValuesColumn,
		MapMetadataName: AttributesMetadataColumn,
		Type:            NewBaseType("Int64"),
	}
}

func NewDefaultFloat64Attribute() Attribute {
	return Attribute{
		KeysArrayName:   "attributes_float64_key",
		ValuesArrayName: "attributes_float64_value",
		TypesArrayName:  "attributes_float64_type",
		MapValueName:    AttributesValuesColumn,
		MapMetadataName: AttributesMetadataColumn,
		Type:            NewBaseType("Float64"),
	}
}

func NewDefaultBoolAttribute() Attribute {
	return Attribute{
		KeysArrayName:   "attributes_bool_key",
		ValuesArrayName: "attributes_bool_value",
		TypesArrayName:  "attributes_bool_type",
		MapValueName:    AttributesValuesColumn,
		MapMetadataName: AttributesMetadataColumn,
		Type:            NewBaseType("Bool"),
	}
}

func (dt DateTimeType) String() string {
	return []string{"DateTime64", "DateTime", "Invalid"}[dt]
}
