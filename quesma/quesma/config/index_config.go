// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
)

type IndexConfiguration struct {
	Name     string `koanf:"name"`
	Disabled bool   `koanf:"disabled"`
	// TODO to be deprecated
	TimestampField    *string                           `koanf:"timestampField"`
	SchemaOverrides   *SchemaConfiguration              `koanf:"schemaOverrides"`
	EnabledOptimizers map[string]OptimizerConfiguration `koanf:"optimizers"`
	Override          string                            `koanf:"override"`
	UseSingleTable    bool                              `koanf:"useSingleTable"`
}

func (c IndexConfiguration) GetTimestampField() (tsField string) {
	if c.TimestampField != nil {
		tsField = *c.TimestampField
	}
	return
}

func (c IndexConfiguration) String() string {
	var str = fmt.Sprintf("\n\t\t%s, disabled: %t, schema overrides: %s, override: %s, useSingleTable: %t",
		c.Name,
		c.Disabled,
		c.SchemaOverrides.String(),
		c.Override,
		c.UseSingleTable,
	)

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
