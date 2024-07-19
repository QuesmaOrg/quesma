// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"slices"
	"strings"
)

type IndexConfiguration struct {
	Name    string `koanf:"name"`
	Enabled bool   `koanf:"enabled"`
	// TODO to be deprecated
	FullTextFields []string `koanf:"fullTextFields"`
	// TODO to be deprecated
	Aliases map[string]FieldAlias `koanf:"aliases"`
	// TODO to be deprecated
	TypeMappings map[string]string `koanf:"mappings"`
	// TODO to be deprecated
	IgnoredFields map[string]bool `koanf:"ignoredFields"`
	// TODO to be deprecated
	TimestampField *string `koanf:"timestampField"`
	// this is hidden from the user right now
	// deprecated
	SchemaConfiguration *SchemaConfiguration              `koanf:"static-schema"`
	EnabledOptimizers   map[string]OptimizerConfiguration `koanf:"optimizers"`
	Override            string                            `koanf:"override"`
}

func (c IndexConfiguration) HasFullTextField(fieldName string) bool {
	return slices.Contains(c.FullTextFields, fieldName)
}

func (c IndexConfiguration) GetTimestampField() (tsField string) {
	if c.TimestampField != nil {
		tsField = *c.TimestampField
	}
	return
}

func (c IndexConfiguration) String() string {
	var extraString string
	extraString = ""
	if len(c.Aliases) > 0 {
		extraString += "; aliases: "
		var aliases []string
		for _, alias := range c.Aliases {
			aliases = append(aliases, fmt.Sprintf("%s <- %s", alias.SourceFieldName, alias.TargetFieldName))
		}
		extraString += strings.Join(aliases, ", ")
	}
	if len(c.IgnoredFields) > 0 {
		extraString += "; ignored fields: "
		var fields []string
		for field := range c.IgnoredFields {
			fields = append(fields, field)
		}
		extraString += strings.Join(fields, ", ")
	}
	var str = fmt.Sprintf("\n\t\t%s, enabled: %t, static-schema: %v, override: %s",
		c.Name,
		c.Enabled,
		c.SchemaConfiguration,
		c.Override,
	)

	if len(c.FullTextFields) > 0 {
		str = fmt.Sprintf("%s, fullTextFields: %s", str, strings.Join(c.FullTextFields, ", "))
	}

	if c.TimestampField != nil {
		return fmt.Sprintf("%s, timestampField: %s", str, *c.TimestampField)
	} else {
		return str
	}
}
