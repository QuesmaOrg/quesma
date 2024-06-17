package elastic_clickhouse_fields

import (
	"fmt"
	"mitmproxy/quesma/jsonprocessor"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/plugins"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/types"
	"strings"
)

// implementations

var doubleColons = "::"
var dot = "."
var sqlNative = "__"

type translateFunc func(string) string

func doubleColons2dot(intput string) string {
	return strings.ReplaceAll(intput, doubleColons, dot)
}

func dot2DoubleColons(intput string) string {
	return strings.ReplaceAll(intput, dot, doubleColons)
}

func dot2SQLNative(intput string) string {
	return strings.ReplaceAll(intput, dot, sqlNative)
}

func sqlNative2Dot(intput string) string {
	return strings.ReplaceAll(intput, sqlNative, dot)
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

type exprColumnNameReplaceVisitor struct {
	translate translateFunc
}

func (v *exprColumnNameReplaceVisitor) visitChildren(args []model.Expr) []model.Expr {
	var newArgs []model.Expr
	for _, arg := range args {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(model.Expr))
		}
	}
	return newArgs
}

func (v *exprColumnNameReplaceVisitor) VisitLiteral(e model.LiteralExpr) interface{} {
	return model.NewLiteral(e.Value)
}

func (v *exprColumnNameReplaceVisitor) VisitInfix(e model.InfixExpr) interface{} {
	lhs := e.Left.Accept(v)
	rhs := e.Right.Accept(v)

	return model.NewInfixExpr(lhs.(model.Expr), e.Op, rhs.(model.Expr))
}

func (v *exprColumnNameReplaceVisitor) VisitPrefixExpr(e model.PrefixExpr) interface{} {
	return model.NewPrefixExpr(e.Op, v.visitChildren(e.Args))
}

func (v *exprColumnNameReplaceVisitor) VisitFunction(e model.FunctionExpr) interface{} {
	return model.NewFunction(e.Name, v.visitChildren(e.Args)...)
}

func (v *exprColumnNameReplaceVisitor) VisitColumnRef(e model.ColumnRef) interface{} {
	return model.NewColumnRef(v.translate(e.ColumnName))
}

func (v *exprColumnNameReplaceVisitor) VisitNestedProperty(e model.NestedProperty) interface{} {
	ColumnRef := e.ColumnRef.Accept(v).(model.ColumnRef)
	Property := e.PropertyName.Accept(v).(model.LiteralExpr)
	return model.NewNestedProperty(ColumnRef, Property)
}

func (v *exprColumnNameReplaceVisitor) VisitArrayAccess(e model.ArrayAccess) interface{} {
	columnRef := e.ColumnRef.Accept(v).(model.ColumnRef)
	index := e.Index.Accept(v).(model.Expr)
	return model.NewArrayAccess(columnRef, index)
}

func (v *exprColumnNameReplaceVisitor) VisitMultiFunction(e model.MultiFunctionExpr) interface{} {
	return model.MultiFunctionExpr{Name: e.Name, Args: v.visitChildren(e.Args)}
}

func (v *exprColumnNameReplaceVisitor) VisitString(e model.StringExpr) interface{} { return e }

func (v *exprColumnNameReplaceVisitor) VisitTableRef(e model.TableRef) interface{} {
	return e
}

func (v *exprColumnNameReplaceVisitor) VisitOrderByExpr(e model.OrderByExpr) interface{} {
	return model.OrderByExpr{Exprs: v.visitChildren(e.Exprs), Direction: e.Direction}
}

func (v *exprColumnNameReplaceVisitor) VisitDistinctExpr(e model.DistinctExpr) interface{} {
	return model.DistinctExpr{Expr: e.Expr.Accept(v).(model.Expr)}
}

func (v *exprColumnNameReplaceVisitor) VisitAliasedExpr(e model.AliasedExpr) interface{} {
	return model.NewAliasedExpr(e.Expr.Accept(v).(model.Expr), e.Alias)
}

func (v *exprColumnNameReplaceVisitor) VisitWindowFunction(f model.WindowFunction) interface{} {
	return model.WindowFunction{
		Name:        f.Name,
		Args:        v.visitChildren(f.Args),
		PartitionBy: v.visitChildren(f.PartitionBy),
		OrderBy:     f.OrderBy.Accept(v).(model.OrderByExpr),
	}
}

func (v *exprColumnNameReplaceVisitor) VisitSelectCommand(query model.SelectCommand) interface{} {

	if query.WhereClause != nil {
		query.WhereClause = query.WhereClause.Accept(v).(model.Expr)
	}

	for i, group := range query.GroupBy {
		query.GroupBy[i] = group.Accept(v).(model.Expr)
	}

	for i, column := range query.Columns {
		query.Columns[i] = column.Accept(v).(model.Expr)
	}

	for i, order := range query.OrderBy {
		query.OrderBy[i] = order.Accept(v).(model.OrderByExpr)
	}

	return query
}

type queryTransformer struct {
	translate translateFunc
}

func (t *queryTransformer) Transform(queries []*model.Query) ([]*model.Query, error) {

	visitor := &exprColumnNameReplaceVisitor{translate: t.translate}

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
	return fmt.Sprintf("%s%s%s", namespace, t.separator, columnName)
}

// plugin definitions

type Dot2DoubleColons struct{}

func (p *Dot2DoubleColons) matches(table string) bool {
	return !strings.HasPrefix(table, "kibana_")
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
	return false
}

func (*Dot2DoubleColons2Dot) IngestTransformer() plugins.IngestTransformer {
	return &ingestTransformer{separator: doubleColons}
}

func (p *Dot2DoubleColons2Dot) ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.IngestTransformer) []plugins.IngestTransformer {
	if p.matches(table) {
		transformers = append(transformers, &ingestTransformer{separator: doubleColons})
	}
	return transformers
}

func (p *Dot2DoubleColons2Dot) GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration) plugins.TableColumNameFormatter {
	if p.matches(table) {
		return &columNameFormatter{separator: doubleColons}
	}
	return nil
}

func (p *Dot2DoubleColons2Dot) ApplyQueryTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.QueryTransformer) []plugins.QueryTransformer {
	if p.matches(table) {
		transformers = append(transformers, &queryTransformer{translate: dot2DoubleColons})
	}
	return transformers
}

func (p *Dot2DoubleColons2Dot) ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.ResultTransformer) []plugins.ResultTransformer {
	if p.matches(table) {
		transformers = append(transformers, &resultTransformer{translate: doubleColons2dot})
	}
	return transformers
}

func (p *Dot2DoubleColons2Dot) ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, transformers []plugins.FieldCapsTransformer) []plugins.FieldCapsTransformer {
	if p.matches(table) {
		transformers = append(transformers, &fieldCapsTransformer{translate: doubleColons2dot})
	}
	return transformers
}

// ultimate solution

type Dot2DoubleUnderscores2Dot struct{}

func (p *Dot2DoubleUnderscores2Dot) matches(table string) bool {

	return strings.HasPrefix(table, "kibana_")
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
