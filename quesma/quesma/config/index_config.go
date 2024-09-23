// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"slices"
)

const (
	ElasticsearchTarget = "elasticsearch"
	ClickhouseTarget    = "clickhouse"
)

type IndexConfiguration struct {
	SchemaOverrides *SchemaConfiguration              `koanf:"schemaOverrides"`
	Optimizers      map[string]OptimizerConfiguration `koanf:"optimizers"`
	Override        string                            `koanf:"override"`
	UseCommonTable  bool                              `koanf:"useCommonTable"`
	Target          []string                          `koanf:"target"`

	// Computed based on the overall configuration
	Name         string
	QueryTarget  []string
	IngestTarget []string
}

func (c IndexConfiguration) String() string {
	var str = fmt.Sprintf("\n\t\t%s, query targets: %v, ingest targets: %v, schema overrides: %s, override: %s, useSingleTable: %t",
		c.Name,
		c.QueryTarget,
		c.IngestTarget,
		c.SchemaOverrides.String(),
		c.Override,
		c.UseCommonTable,
	)

	return str
}

func (c IndexConfiguration) GetOptimizerConfiguration(optimizerName string) (props map[string]string, disabled bool) {
	if optimizer, ok := c.Optimizers[optimizerName]; ok {
		return optimizer.Properties, optimizer.Disabled
	}
	return nil, true
}

func (c IndexConfiguration) IsElasticQueryEnabled() bool {
	return slices.Contains(c.QueryTarget, ElasticsearchTarget)
}

func (c IndexConfiguration) IsElasticIngestEnabled() bool {
	return slices.Contains(c.IngestTarget, ElasticsearchTarget)
}

func (c IndexConfiguration) IsClickhouseQueryEnabled() bool {
	return slices.Contains(c.QueryTarget, ClickhouseTarget)
}

func (c IndexConfiguration) IsClickhouseIngestEnabled() bool {
	return slices.Contains(c.IngestTarget, ClickhouseTarget)
}
