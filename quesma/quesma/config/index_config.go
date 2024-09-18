// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
)

const (
	ElasticsearchTarget = "elasticsearch"
	ClickhouseTarget    = "clickhouse"
)

type IndexConfiguration struct {
	Name            string                            `koanf:"name"`
	Disabled        bool                              `koanf:"disabled"`
	SchemaOverrides *SchemaConfiguration              `koanf:"schemaOverrides"`
	Optimizers      map[string]OptimizerConfiguration `koanf:"optimizers"`
	Override        string                            `koanf:"override"`
	UseSingleTable  bool                              `koanf:"useSingleTable"`
	Target          []string                          `koanf:"target"`

	// Computed based on the overall configuration
	QueryTarget  []string
	IngestTarget []string
}

func (c IndexConfiguration) String() string {
	var str = fmt.Sprintf("\n\t\t%s, disabled: %t, schema overrides: %s, override: %s, useSingleTable: %t",
		c.Name,
		c.Disabled,
		c.SchemaOverrides.String(),
		c.Override,
		c.UseSingleTable,
	)

	return str
}

func (c IndexConfiguration) GetOptimizerConfiguration(optimizerName string) (props map[string]string, disabled bool) {
	if optimizer, ok := c.Optimizers[optimizerName]; ok {
		return optimizer.Properties, optimizer.Disabled
	}
	return nil, true
}
