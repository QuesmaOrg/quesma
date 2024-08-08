// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package registry

import (
	"fmt"
	"quesma/plugins"
	"quesma/quesma/config"
	"quesma/schema"
)

// Plugin changes the behavior of Quesma by changing the pipeline of transformers
type Plugin interface {
	ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.FieldCapsTransformer) []plugins.FieldCapsTransformer
	ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.ResultTransformer) []plugins.ResultTransformer
	GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration, schema schema.Registry) plugins.TableColumNameFormatter
}

var registeredPlugins []Plugin

func init() {
	registeredPlugins = []Plugin{}
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
