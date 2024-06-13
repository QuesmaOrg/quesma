package elastic_clickhouse_fields

import (
	"fmt"
	"mitmproxy/quesma/jsonprocessor"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/plugins"
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

func identity(input string) string {
	return input
}

type resultTransformer struct {
	translate translateFunc
}

func (t *resultTransformer) Transform(rows []model.QueryResultRow) ([]model.QueryResultRow, error) {
	for i, row := range rows {
		for j, _ := range row.Cols {
			rows[i].Cols[j].ColName = t.translate(row.Cols[j].ColName)
		}
	}
	return rows, nil
}

type fieldCapsTransformer struct {
	translate translateFunc
}

func (t *fieldCapsTransformer) Transform(fieldCaps model.FieldCapsResponse) (model.FieldCapsResponse, error) {
	for name, fields := range fieldCaps.Fields {
		newName := t.translate(name)

		if _, ok := fieldCaps.Fields[newName]; !ok {
			fieldCaps.Fields[newName] = fields
			delete(fieldCaps.Fields, name)
		}
	}
	return fieldCaps, nil
}

// query transformer

type exprColumnNameReplaceVisitor struct {
	translate translateFunc
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
	var newArgs []model.Expr
	for _, arg := range e.Args {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(model.Expr))
		}
	}
	return model.NewPrefixExpr(e.Op, newArgs)
}

func (v *exprColumnNameReplaceVisitor) VisitFunction(e model.FunctionExpr) interface{} {
	var newArgs []model.Expr
	for _, arg := range e.Args {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(model.Expr))
		}
	}
	return model.NewFunction(e.Name, newArgs...)
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
	var newArgs []model.Expr
	for _, arg := range e.Args {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(model.Expr))
		}
	}
	return model.MultiFunctionExpr{Name: e.Name, Args: e.Args}
}

func (v *exprColumnNameReplaceVisitor) VisitString(e model.StringExpr) interface{} { return e }
func (v *exprColumnNameReplaceVisitor) VisitSQL(e model.SQL) interface{}           { return e }

func (v *exprColumnNameReplaceVisitor) VisitTableRef(e model.TableRef) interface{} {
	return e
}

func (v *exprColumnNameReplaceVisitor) VisitOrderByExpr(e model.OrderByExpr) interface{} {
	var newArgs []model.Expr
	for _, arg := range e.Exprs {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(model.Expr))
		}
	}
	return model.OrderByExpr{Exprs: newArgs, Direction: e.Direction}
}

func (v *exprColumnNameReplaceVisitor) VisitDistinctExpr(e model.DistinctExpr) interface{} {
	return model.DistinctExpr{Expr: e.Accept(v).(model.Expr)}
}

type queryTransformer struct {
	translate translateFunc
}

func (t *queryTransformer) Transform(queries []*model.Query) ([]*model.Query, error) {

	visitor := &exprColumnNameReplaceVisitor{translate: t.translate}

	for _, query := range queries {

		//  waiting for a  "select query visitor"

		if query.WhereClause != nil {
			query.WhereClause = query.WhereClause.Accept(visitor).(model.Expr)
		}

		for i, group := range query.GroupBy {
			query.GroupBy[i] = group.Accept(visitor).(model.Expr)
		}

		for i, column := range query.Columns {
			query.Columns[i].Expression = column.Expression.Accept(visitor).(model.Expr)
		}

		for i, order := range query.OrderBy {
			for j := range order.Exprs {
				query.OrderBy[i].Exprs[j] = order.Exprs[j].Accept(visitor).(model.Expr)
			}
		}
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

type LegacyClickhouseDoubleColonsPlugin struct{}

func (*LegacyClickhouseDoubleColonsPlugin) ResultTransformer() plugins.ResultTransformer {
	return &plugins.NopResultTransformer{}
}

func (*LegacyClickhouseDoubleColonsPlugin) FieldCapsTransformer() plugins.FieldCapsTransformer {
	return &plugins.NopFieldCapsTransformer{}
}

func (*LegacyClickhouseDoubleColonsPlugin) QueryTransformer() plugins.QueryTransformer {
	return &plugins.NopQueryTransformer{}
}

func (*LegacyClickhouseDoubleColonsPlugin) IngestTransformer() plugins.IngestTransformer {
	return &ingestTransformer{separator: doubleColons}
}

func (*LegacyClickhouseDoubleColonsPlugin) TableColumNameFormatter() plugins.TableColumNameFormatter {
	return &columNameFormatter{separator: doubleColons}
}

// temporary solution for indexes stored with "::" separator

type ClickhouseDoubleColonsElasticDotsPlugin struct{}

func (*ClickhouseDoubleColonsElasticDotsPlugin) ResultTransformer() plugins.ResultTransformer {
	return &resultTransformer{translate: doubleColons2dot}
}

func (*ClickhouseDoubleColonsElasticDotsPlugin) FieldCapsTransformer() plugins.FieldCapsTransformer {
	return &fieldCapsTransformer{translate: doubleColons2dot}
}

func (*ClickhouseDoubleColonsElasticDotsPlugin) QueryTransformer() plugins.QueryTransformer {
	return &queryTransformer{translate: dot2DoubleColons}
}

func (*ClickhouseDoubleColonsElasticDotsPlugin) IngestTransformer() plugins.IngestTransformer {
	return &ingestTransformer{separator: doubleColons}
}

func (*ClickhouseDoubleColonsElasticDotsPlugin) TableColumNameFormatter() plugins.TableColumNameFormatter {
	return &columNameFormatter{separator: doubleColons}
}

// ultimate solution

type ClickhouseSQLNativeLElasticDotsPlugin struct{}

func (*ClickhouseSQLNativeLElasticDotsPlugin) ResultTransformer() plugins.ResultTransformer {
	return &resultTransformer{translate: sqlNative2Dot}
}

func (*ClickhouseSQLNativeLElasticDotsPlugin) FieldCapsTransformer() plugins.FieldCapsTransformer {
	return &fieldCapsTransformer{translate: sqlNative2Dot}
}

func (*ClickhouseSQLNativeLElasticDotsPlugin) QueryTransformer() plugins.QueryTransformer {
	return &queryTransformer{translate: dot2SQLNative}
}

func (*ClickhouseSQLNativeLElasticDotsPlugin) IngestTransformer() plugins.IngestTransformer {
	return &ingestTransformer{separator: sqlNative}
}

func (*ClickhouseSQLNativeLElasticDotsPlugin) TableColumNameFormatter() plugins.TableColumNameFormatter {
	return &columNameFormatter{separator: sqlNative}
}
