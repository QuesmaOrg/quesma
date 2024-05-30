package config

import (
	"fmt"
	"strings"
)

type (
	// not yet in use
	SchemaConfiguration struct {
		Fields                    map[FieldName]FieldConfiguration `koanf:"fields"`
		Ignored                   []string                         `koanf:"ignored"`
		UnknownPropertiesStrategy UnknownPropertiesConfiguration   `koanf:"unknown-fields"`
	}
	FieldConfiguration struct {
		Name FieldName `koanf:"name"`
		// when 'alias' used, other fields are inherited from the aliased field
		Type         FieldType `koanf:"type"`
		IsPrimaryKey bool      `koanf:"primary-key"`
		// target column name, if different than the field name, can point to 'attributes'
		ColumnName   string `koanf:"column-name"`
		AliasedField string `koanf:"aliased-field"`
	}
	FieldName                      string
	FieldType                      string
	UnknownPropertiesConfiguration struct {
		Strategy            UnknownPropertiesStrategy `koanf:"strategy"`
		MatchKeywordAndText bool                      `koanf:"match-keyword-and-text"`
	}
	UnknownPropertiesStrategy string
)

func (ft FieldType) AsString() string {
	return string(ft)
}

func (fn FieldName) AsString() string {
	return string(fn)
}

func (fc FieldConfiguration) String() string {
	baseString := fmt.Sprintf("FieldConfiguration: Name=%s, Type=%s", fc.Name, fc.Type)
	if fc.ColumnName != "" {
		baseString += fmt.Sprintf(", ColumnName=%s", fc.ColumnName)
	}
	return baseString
}

func (sc *SchemaConfiguration) String() string {
	var builder strings.Builder

	builder.WriteString("SchemaConfiguration:\n")

	builder.WriteString("Fields:\n")
	for fieldName, fieldConfig := range sc.Fields {
		builder.WriteString(fmt.Sprintf("\t%s: %+v\n", fieldName, fieldConfig))
	}

	builder.WriteString("Ignored:\n")
	for _, ignoredField := range sc.Ignored {
		builder.WriteString(fmt.Sprintf("\t%s\n", ignoredField))
	}

	builder.WriteString(fmt.Sprintf("UnknownPropertiesStrategy: %+v\n", sc.UnknownPropertiesStrategy))
	return builder.String()
}

func NewEmptySchemaConfiguration() SchemaConfiguration {
	return SchemaConfiguration{Fields: make(map[FieldName]FieldConfiguration)}
}
