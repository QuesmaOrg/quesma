// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_clickhouse_fields

import (
	"fmt"
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

type fieldCapsTransformer struct {
	translate translateFunc
}

func (t *fieldCapsTransformer) Transform(fieldCaps map[string]map[string]model.FieldCapability) (map[string]map[string]model.FieldCapability, error) {
	for name, fields := range fieldCaps {
		newName := t.translate(name)

		if _, ok := fieldCaps[newName]; !ok {
			fieldCaps[newName] = fields
			delete(fieldCaps, name)
		}
	}
	return fieldCaps, nil
}

// query transformer

//

//

type columNameFormatter struct {
	separator string
}

func (t *columNameFormatter) Format(namespace, columnName string) string {
	if namespace == "" {
		return columnName
	}
	return fmt.Sprintf("%s%s%s", namespace, t.separator, columnName)
}

// plugin definitions

// temporary solution for indexes stored with "::" separator

type Dot2DoubleColons2Dot struct{}

func (*Dot2DoubleColons2Dot) matches(table string) bool {
	return true
}

func (p *Dot2DoubleColons2Dot) GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration, schema schema.Registry) plugins.TableColumNameFormatter {
	if p.matches(table) {
		return &columNameFormatter{separator: doubleColons}
	}
	return nil
}

func (p *Dot2DoubleColons2Dot) ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.ResultTransformer) []plugins.ResultTransformer {
	if p.matches(table) {
		transformers = append(transformers, &resultTransformer{translate: doubleColons2dot})
	}
	return transformers
}

func (p *Dot2DoubleColons2Dot) ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.FieldCapsTransformer) []plugins.FieldCapsTransformer {
	return transformers
}
