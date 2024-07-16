// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/model"
	"quesma/quesma/config"
	"quesma/schema"
	"testing"
)

func Test_index_table_mapping(t *testing.T) {
	expectedQueries := []*model.Query{
		{
			TableName: "kibana_sample_data_logs",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
			},
		},
	}

	queries := [][]*model.Query{
		{
			{
				TableName: "kibana_sample_data_logs",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs"),
				}},
		},
	}

	indexConfig := map[string]config.IndexConfiguration{
		"kibana_sample_data_logs": {
			Name:    "kibana_sample_data_logs",
			Enabled: true,
		},
	}

	cfg := config.QuesmaConfiguration{
		IndexConfig: indexConfig,
	}

	tableDiscovery :=
		fixedTableProvider{tables: map[string]schema.Table{
			"kibana_sample_data_flights": {Columns: map[string]schema.Column{
				"DestLocation": {Name: "DestLocation", Type: "geo_point"},
				"clientip":     {Name: "clientip", Type: "ip"},
			}},
		}}
	s := schema.NewSchemaRegistry(tableDiscovery, cfg, clickhouse.SchemaTypeAdapter{})
	transform := &SchemaCheckPass{cfg: indexConfig, schemaRegistry: s, logManager: clickhouse.NewLogManagerEmpty()}

	for k := range queries {
		resultQueries, err := transform.Transform(queries[k])
		assert.NoError(t, err)
		assert.Equal(t, expectedQueries[k].SelectCommand.String(), resultQueries[0].SelectCommand.String())
	}
}
