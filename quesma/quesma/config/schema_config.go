// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"strings"
)

const TypeAlias = "alias"

type (
	SchemaConfiguration struct {
		Fields map[FieldName]FieldConfiguration `koanf:"fields"`
	}
	FieldConfiguration struct {
		Type FieldType `koanf:"type"`
		//IsTimestampField bool      `koanf:"isTimestampField"`
		TargetColumnName string `koanf:"targetColumnName"` // if FieldType == TypeAlias then this is the target column name
		Ignored          bool   `koanf:"ignored"`
	}
	FieldName string
	FieldType string
)

func (ft FieldType) AsString() string {
	return string(ft)
}

func (fn FieldName) AsString() string {
	return string(fn)
}

func (fc FieldConfiguration) String() string {
	baseString := fmt.Sprintf("Type=%s", fc.Type)
	if fc.TargetColumnName != "" {
		baseString += fmt.Sprintf(", TargetColumnName=%s", fc.TargetColumnName)
	}
	if fc.Ignored {
		baseString += ", Ignored"
	}
	return baseString
}

func (sc *SchemaConfiguration) String() string {
	if sc == nil {
		return "NO SCHEMA OVERRIDES"
	}
	var builder strings.Builder

	addComma := false
	for fieldName, fieldConfig := range sc.Fields {
		if addComma {
			builder.WriteString(", ")
		} else {
			addComma = true
		}
		builder.WriteString(fmt.Sprintf("%s: %v", fieldName, fieldConfig))
	}
	return builder.String()
}

func (sc *SchemaConfiguration) IgnoredFields() []FieldName {
	var ignoredFields []FieldName
	for fieldName, fieldConfig := range sc.Fields {
		if fieldConfig.Ignored {
			ignoredFields = append(ignoredFields, fieldName)
		}
	}
	return ignoredFields
}

func NewEmptySchemaConfiguration() SchemaConfiguration {
	return SchemaConfiguration{Fields: make(map[FieldName]FieldConfiguration)}
}
