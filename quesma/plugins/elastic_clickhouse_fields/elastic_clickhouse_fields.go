// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_clickhouse_fields

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/jsonprocessor"
	"quesma/model"
	"quesma/plugins"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"strings"
)

// implementations

var doubleColons = "::"
var dot = "."
var sqlNative = "__"

type translateFunc func(string) string

func doubleColons2dot(input string) string {
	return strings.ReplaceAll(input, doubleColons, dot)
}

func dot2SQLNative(input string) string {
	return strings.ReplaceAll(input, dot, sqlNative)
}

func sqlNative2Dot(input string) string {
	return strings.ReplaceAll(input, sqlNative, dot)
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

type queryTransformer struct {
	translate translateFunc
}

func newColumnNameTranslator(translate translateFunc) model.ExprVisitor {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {

		return model.NewColumnRef(translate(e.ColumnName))
	}

	return visitor

}

func (t *queryTransformer) Transform(queries []*model.Query) ([]*model.Query, error) {

	visitor := newColumnNameTranslator(t.translate)

	for _, query := range queries {
		query.SelectCommand = query.SelectCommand.Accept(visitor).(model.SelectCommand)
	}

	return queries, nil
}

//

type ingestTransformer struct {
	separator string
}

func (t *ingestTransformer) Transform(document types.JSON) (types.JSON, error) {
	return jsonprocessor.FlattenMap(document, t.separator), nil
}

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

type Dot2DoubleColons struct{}

func (p *Dot2DoubleColons) matches(table string) bool {
	// TODO this breaks geo stuff,
	// so we disable it for e-commerce data
	return !strings.HasPrefix(table, "kibana_sample_data_ecommerce")
}

func (p *Dot2DoubleColons) ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.IngestTransformer) []plugins.IngestTransformer {
	if p.matches(table) {
		transformers = append(transformers, &ingestTransformer{separator: doubleColons})
	}
	return transformers
}

func (p *Dot2DoubleColons) ApplyQueryTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.QueryTransformer) []plugins.QueryTransformer {
	return transformers
}

func (p *Dot2DoubleColons) ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.ResultTransformer) []plugins.ResultTransformer {
	return transformers
}

func (p *Dot2DoubleColons) ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.FieldCapsTransformer) []plugins.FieldCapsTransformer {
	return transformers
}

func (p *Dot2DoubleColons) GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration) plugins.TableColumNameFormatter {
	if p.matches(table) {
		return &columNameFormatter{separator: doubleColons}
	}
	return nil
}

// temporary solution for indexes stored with "::" separator

type Dot2DoubleColons2Dot struct{}

func (*Dot2DoubleColons2Dot) matches(table string) bool {
	return true
}

func (*Dot2DoubleColons2Dot) IngestTransformer() plugins.IngestTransformer {
	return &ingestTransformer{separator: doubleColons}
}

func (p *Dot2DoubleColons2Dot) ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, tableMap clickhouse.TableMap, transformers []plugins.IngestTransformer) []plugins.IngestTransformer {
	if p.matches(table) {
		transformers = append(transformers, &ingestTransformer{separator: doubleColons})
	}
	return transformers
}

func (p *Dot2DoubleColons2Dot) GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration, schema schema.Registry) plugins.TableColumNameFormatter {
	if p.matches(table) {
		return &columNameFormatter{separator: doubleColons}
	}
	return nil
}

func (p *Dot2DoubleColons2Dot) ApplyQueryTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.QueryTransformer) []plugins.QueryTransformer {
	return transformers
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

// ultimate solution

type Dot2DoubleUnderscores2Dot struct{}

func (p *Dot2DoubleUnderscores2Dot) matches(table string) bool {
	return false
}

func (p *Dot2DoubleUnderscores2Dot) ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.IngestTransformer) []plugins.IngestTransformer {
	if p.matches(table) {
		transformers = append(transformers, &ingestTransformer{separator: doubleColons})
	}
	return transformers
}

func (p *Dot2DoubleUnderscores2Dot) GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration) plugins.TableColumNameFormatter {
	if p.matches(table) {
		return &columNameFormatter{separator: sqlNative}
	}
	return nil
}

func (p *Dot2DoubleUnderscores2Dot) ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.FieldCapsTransformer) []plugins.FieldCapsTransformer {
	if p.matches(table) {
		transformers = append(transformers, &fieldCapsTransformer{translate: sqlNative2Dot})
	}
	return transformers
}

func (p *Dot2DoubleUnderscores2Dot) ApplyQueryTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.QueryTransformer) []plugins.QueryTransformer {
	if p.matches(table) {
		transformers = append(transformers, &queryTransformer{translate: dot2SQLNative})
	}
	return transformers
}

func (p *Dot2DoubleUnderscores2Dot) ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.ResultTransformer) []plugins.ResultTransformer {
	if p.matches(table) {
		transformers = append(transformers, &resultTransformer{translate: sqlNative2Dot})
	}
	return transformers
}
