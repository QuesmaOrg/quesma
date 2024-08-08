// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package registry

import (
	"quesma/plugins"
	"quesma/quesma/config"
	"quesma/schema"
)

// Plugin changes the behavior of Quesma by changing the pipeline of transformers
type Plugin interface {
	ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.ResultTransformer) []plugins.ResultTransformer
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
