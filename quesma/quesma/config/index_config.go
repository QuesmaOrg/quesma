// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"strings"
)

type IndexConfiguration struct {
	Name     string `koanf:"name"`
	Disabled bool   `koanf:"disabled"`
	// TODO to be deprecated
	FullTextFields []string `koanf:"fullTextFields"`
	// TODO to be deprecated
	IgnoredFields     map[string]bool                   `koanf:"ignoredFields"`
	SchemaOverrides   *SchemaConfiguration              `koanf:"schemaOverrides"`
	EnabledOptimizers map[string]OptimizerConfiguration `koanf:"optimizers"`
	Override          string                            `koanf:"override"`
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
	var str = fmt.Sprintf("\n\t\t%s, disabled: %t, schema overrides: %s, override: %s",
		c.Name,
		c.Disabled,
		c.SchemaOverrides.String(),
		c.Override,
	)

	if len(c.FullTextFields) > 0 {
		str = fmt.Sprintf("%s, fullTextFields: %s", str, strings.Join(c.FullTextFields, ", "))
	}

	return str
}

func (c IndexConfiguration) GetOptimizerConfiguration(optimizerName string) (map[string]string, bool) {
	if optimizer, ok := c.EnabledOptimizers[optimizerName]; ok {
		return optimizer.Properties, optimizer.Enabled
	}

	return nil, false
}
