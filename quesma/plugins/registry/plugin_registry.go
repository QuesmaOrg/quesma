// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package registry

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/plugins"
	"quesma/plugins/elastic_clickhouse_fields"
	"quesma/plugins/ingest_validator"
	"quesma/quesma/config"
	"quesma/schema"
)

var registeredPlugins []plugins.Plugin

func init() {
	registeredPlugins = []plugins.Plugin{&ingest_validator.IngestValidator{}, &elastic_clickhouse_fields.Dot2DoubleColons2Dot{}}
}

func QueryTransformerFor(table string, cfg config.QuesmaConfiguration, schema schema.Registry) plugins.QueryTransformer {

	var transformers []plugins.QueryTransformer

	for _, plugin := range registeredPlugins {
		transformers = plugin.ApplyQueryTransformers(table, cfg, schema, transformers)
	}

	if len(transformers) == 0 {
		return &plugins.NopQueryTransformer{}
	}

	return plugins.QueryTransformerPipeline(transformers)
}

///

func ResultTransformerFor(table string, cfg config.QuesmaConfiguration, schema schema.Registry) plugins.ResultTransformer {

	var transformers []plugins.ResultTransformer

	for _, plugin := range registeredPlugins {
		transformers = plugin.ApplyResultTransformers(table, cfg, schema, transformers)
	}

	if len(transformers) == 0 {
		return &plugins.NopResultTransformer{}
	}

	return plugins.ResultTransformerPipeline(transformers)
}

///

func FieldCapsTransformerFor(table string, cfg config.QuesmaConfiguration, schema schema.Registry) plugins.FieldCapsTransformer {

	var transformers []plugins.FieldCapsTransformer

	for _, plugin := range registeredPlugins {
		transformers = plugin.ApplyFieldCapsTransformers(table, cfg, schema, transformers)
	}

	if len(transformers) == 0 {
		return &plugins.NopFieldCapsTransformer{}
	}

	return plugins.FieldCapsTransformerPipeline(transformers)
}

func TableColumNameFormatterFor(table string, cfg config.QuesmaConfiguration, schema schema.Registry) (plugins.TableColumNameFormatter, error) {

	var transformers []plugins.TableColumNameFormatter

	for _, plugin := range registeredPlugins {
		t := plugin.GetTableColumnFormatter(table, cfg, schema)
		if t != nil {
			transformers = append(transformers, t)
		}
	}

	if len(transformers) == 0 {
		return nil, fmt.Errorf("no table column name formatter found for table %s", table)
	}

	if len(transformers) > 1 {
		return nil, fmt.Errorf("multiple table column name formatters are not supported, table %s", table)
	}

	return transformers[0], nil
}

func IngestTransformerFor(table string, cfg config.QuesmaConfiguration, schema schema.Registry, tableMap clickhouse.TableMap) plugins.IngestTransformer {

	var transformers []plugins.IngestTransformer

	for _, plugin := range registeredPlugins {
		transformers = plugin.ApplyIngestTransformers(table, cfg, schema, transformers)
	}

	if len(transformers) == 0 {
		return &plugins.NopIngestTransformer{}
	}

	return plugins.IngestTransformerPipeline(transformers)
}
