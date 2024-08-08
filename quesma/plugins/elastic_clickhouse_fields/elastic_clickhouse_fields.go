// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_clickhouse_fields

import (
	"quesma/model"
	"quesma/plugins"
	"quesma/quesma/config"
	"quesma/schema"
	"strings"
)

// implementations

var doubleColons = "::"
var dot = "."

type translateFunc func(string) string

func doubleColons2dot(input string) string {
	return strings.ReplaceAll(input, doubleColons, dot)
}

type resultTransformer struct {
	translate translateFunc
}

func (t *resultTransformer) Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {

	for _, rows := range result {
		for i, row := range rows {
			for j := range row.Cols {
				rows[i].Cols[j].ColName = t.translate(row.Cols[j].ColName)
			}
		}
	}
	return result, nil
}

// plugin definitions

// temporary solution for indexes stored with "::" separator

type Dot2DoubleColons2Dot struct{}

func (*Dot2DoubleColons2Dot) matches(table string) bool {
	return true
}

func (p *Dot2DoubleColons2Dot) ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.ResultTransformer) []plugins.ResultTransformer {
	if p.matches(table) {
		transformers = append(transformers, &resultTransformer{translate: doubleColons2dot})
	}
	return transformers
}
