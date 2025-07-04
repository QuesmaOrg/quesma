// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"slices"
	"strings"
)

const (
	ElasticsearchTarget = "elasticsearch"
	ClickhouseTarget    = "clickhouse"
	DorisTarget         = "doris"
)

// PartitionStrategy represents a configurable partitioning strategy for ClickHouse tables created by Quesma
type PartitionStrategy string

const (
	Hourly  PartitionStrategy = "hourly"
	Daily   PartitionStrategy = "daily"
	Monthly PartitionStrategy = "monthly"
	Yearly  PartitionStrategy = "yearly"
	None    PartitionStrategy = ""
)

type IndexConfiguration struct {
	SchemaOverrides *SchemaConfiguration              `koanf:"schemaOverrides"`
	Optimizers      map[string]OptimizerConfiguration `koanf:"optimizers"`
	Override        string                            `koanf:"tableName"` // use method TableName()
	UseCommonTable  bool                              `koanf:"useCommonTable"`
	Target          any                               `koanf:"target"`

	// PartitioningStrategy adds PARTITION BY clause to the table creation query
	PartitioningStrategy PartitionStrategy `koanf:"partitioningStrategy"` // Experimental feature
	EnableFieldMapSyntax bool              `koanf:"enableFieldMapSyntax"` // Experimental feature

	// Computed based on the overall configuration
	QueryTarget  []string
	IngestTarget []string
}

type OptimizerConfiguration struct {
	Disabled   bool              `koanf:"disabled"`
	Properties map[string]string `koanf:"properties"`
}

func (c IndexConfiguration) TableName(origName string) string {
	if len(c.Override) > 0 {
		return c.Override
	}
	return origName
}

func (c IndexConfiguration) String(indexName string) string {
	var builder strings.Builder

	builder.WriteString("\n\t\t")
	builder.WriteString(indexName)
	builder.WriteString(", query targets: ")
	builder.WriteString(fmt.Sprintf("%v", c.QueryTarget))
	builder.WriteString(", ingest targets: ")
	builder.WriteString(fmt.Sprintf("%v", c.IngestTarget))
	if c.SchemaOverrides != nil && len(c.SchemaOverrides.Fields) > 0 {
		builder.WriteString(",\n\t\t\tschema overrides: ")
		builder.WriteString(c.SchemaOverrides.String())
		builder.WriteString("\n\t\t\t")
	} else {
		builder.WriteString("\n\t\t\t")
	}
	if len(c.Override) > 0 {
		builder.WriteString(", Override: ")
		builder.WriteString(c.Override)
	}
	if c.UseCommonTable {
		builder.WriteString(", useSingleTable: true")
	}
	builder.WriteString(fmt.Sprintf(", enableFieldMapSyntax: %v", c.EnableFieldMapSyntax))

	return builder.String()
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

func (c IndexConfiguration) IsIngestDisabled() bool {
	return len(c.IngestTarget) == 0
}
