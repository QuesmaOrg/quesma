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
	TimestampField  *string                           `koanf:"timestampField"`
	SchemaOverrides *SchemaConfiguration              `koanf:"schemaOverrides"`
	Optimizers      map[string]OptimizerConfiguration `koanf:"optimizers"`
	Override        string                            `koanf:"override"`
}

func (c IndexConfiguration) GetTimestampField() (tsField string) {
	if c.TimestampField != nil {
		tsField = *c.TimestampField
	}
	return
}

func (c IndexConfiguration) String() string {
	var str = fmt.Sprintf("\n\t\t%s, disabled: %t, schema overrides: %s, override: %s",
		c.Name,
		c.Disabled,
		c.SchemaOverrides.String(),
		c.Override,
	)

	if c.TimestampField != nil {
		return fmt.Sprintf("%s, timestampField: %s", str, *c.TimestampField)
	} else {
		return str
	}
}

func (c IndexConfiguration) GetOptimizerConfiguration(optimizerName string) (map[string]string, bool) {
	if optimizer, ok := c.Optimizers[optimizerName]; ok {
		return optimizer.Properties, optimizer.Disabled
	}

	return nil, false
}
