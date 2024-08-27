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
	Enabled bool   `koanf:"enabled"` // TODO rename to `Disabled` to reduce already polluted config
	// TODO to be deprecated
	FullTextFields []string `koanf:"fullTextFields"`
	// TODO to be deprecated
	IgnoredFields map[string]bool `koanf:"ignoredFields"`
	// TODO to be deprecated
	TimestampField *string `koanf:"timestampField"`
	// this is hidden from the user right now
	// deprecated
	SchemaOverrides   *SchemaConfiguration              `koanf:"schemaOverrides"`
	EnabledOptimizers map[string]OptimizerConfiguration `koanf:"optimizers"`
	Override          string                            `koanf:"override"`
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
	if len(c.IgnoredFields) > 0 {
		extraString += "; ignored fields: "
		var fields []string
		for field := range c.IgnoredFields {
			fields = append(fields, field)
		}
		extraString += strings.Join(fields, ", ")
	}
	var str = fmt.Sprintf("\n\t\t%s, enabled: %t, schema overrides: %s, override: %s",
		c.Name,
		c.Enabled,
		c.SchemaOverrides.String(),
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

func (c IndexConfiguration) GetOptimizerConfiguration(optimizerName string) (map[string]string, bool) {
	if optimizer, ok := c.EnabledOptimizers[optimizerName]; ok {
		return optimizer.Properties, optimizer.Enabled
	}

	return nil, false
}
