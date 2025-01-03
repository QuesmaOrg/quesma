// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/logger"
	"quesma/model"
	"quesma/schema"
	"strings"
)

//
//
// Do not use `arrayJoin` here. It's considered harmful.
//
//
//

// Aggregate functions names, generated from ClickHouse documentation:
// git clone --depth 1 https://github.com/ClickHouse/ClickHouse.git
// cd ClickHouse/docs/en/sql-reference/aggregate-functions/reference
// find . -type f | cut -c3- | rev | cut -c4- | rev | sort

var aggregateFunctions = map[string]bool{
	"aggthrow":                     true,
	"analysis_of_variance":         true,
	"any":                          true,
	"anyheavy":                     true,
	"anylast":                      true,
	"approxtopk":                   true,
	"approxtopsum":                 true,
	"argmax":                       true,
	"argmin":                       true,
	"arrayconcatagg":               true,
	"avg":                          true,
	"avgweighted":                  true,
	"boundrat":                     true,
	"categoricalinformationvalue":  true,
	"contingency":                  true,
	"corr":                         true,
	"corrmatrix":                   true,
	"corrstable":                   true,
	"count":                        true,
	"covarpop":                     true,
	"covarpopmatrix":               true,
	"covarpopstable":               true,
	"covarsamp":                    true,
	"covarsampmatrix":              true,
	"covarsampstable":              true,
	"cramersv":                     true,
	"cramersvbiascorrected":        true,
	"deltasum":                     true,
	"deltasumtimestamp":            true,
	"entropy":                      true,
	"exponentialmovingaverage":     true,
	"exponentialtimedecayedavg":    true,
	"exponentialtimedecayedcount":  true,
	"exponentialtimedecayedmax":    true,
	"exponentialtimedecayedsum":    true,
	"first_value":                  true,
	"flame_graph":                  true,
	"grouparray":                   true,
	"grouparrayinsertat":           true,
	"grouparrayintersect":          true,
	"grouparraylast":               true,
	"grouparraymovingavg":          true,
	"grouparraymovingsum":          true,
	"grouparraysample":             true,
	"grouparraysorted":             true,
	"groupbitand":                  true,
	"groupbitmap":                  true,
	"groupbitmapand":               true,
	"groupbitmapor":                true,
	"groupbitmapxor":               true,
	"groupbitor":                   true,
	"groupbitxor":                  true,
	"groupconcat":                  true,
	"groupuniqarray":               true,
	"index":                        true,
	"intervalLengthSum":            true,
	"kolmogorovsmirnovtest":        true,
	"kurtpop":                      true,
	"kurtsamp":                     true,
	"largestTriangleThreeBuckets":  true,
	"last_value":                   true,
	"mannwhitneyutest":             true,
	"max":                          true,
	"maxintersections":             true,
	"maxintersectionsposition":     true,
	"maxmap":                       true,
	"meanztest":                    true,
	"median":                       true,
	"min":                          true,
	"minmap":                       true,
	"quantile":                     true,
	"quantileGK":                   true,
	"quantilebfloat16":             true,
	"quantileddsketch":             true,
	"quantiledeterministic":        true,
	"quantileexact":                true,
	"quantileexactweighted":        true,
	"quantileinterpolatedweighted": true,
	"quantiles":                    true,
	"quantiletdigest":              true,
	"quantiletdigestweighted":      true,
	"quantiletiming":               true,
	"quantiletimingweighted":       true,
	"rankCorr":                     true,
	"simplelinearregression":       true,
	"singlevalueornull":            true,
	"skewpop":                      true,
	"skewsamp":                     true,
	"sparkbar":                     true,
	"stddevpop":                    true,
	"stddevpopstable":              true,
	"stddevsamp":                   true,
	"stddevsampstable":             true,
	"stochasticlinearregression":   true,
	"stochasticlogisticregression": true,
	"studentttest":                 true,
	"sum":                          true,
	"sumcount":                     true,
	"sumkahan":                     true,
	"summap":                       true,
	"summapwithoverflow":           true,
	"sumwithoverflow":              true,
	"theilsu":                      true,
	"topk":                         true,
	"topkweighted":                 true,
	"uniq":                         true,
	"uniqcombined":                 true,
	"uniqcombined64":               true,
	"uniqexact":                    true,
	"uniqhll12":                    true,
	"uniqthetasketch":              true,
	"varpop":                       true,
	"varpopstable":                 true,
	"varsamp":                      true,
	"varsampstable":                true,
	"welchttest":                   true,
}

type arrayTypeResolver struct {
	indexSchema schema.Schema
}

func (v *arrayTypeResolver) dbColumnType(columName string) string {

	//
	// This is a HACK to get the column database type from the schema
	//
	//
	// here we should resolve field by column name not field name
	columName = strings.TrimSuffix(columName, ".keyword")

	field, ok := v.indexSchema.ResolveFieldByInternalName(columName)

	if !ok {
		return ""
	}

	return field.InternalPropertyType
}

func NewArrayTypeVisitor(resolver arrayTypeResolver) model.ExprVisitor {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {

		column, ok := e.Left.(model.ColumnRef)
		if ok {
			dbType := resolver.dbColumnType(column.ColumnName)
			if strings.HasPrefix(dbType, "Array") {
				op := strings.ToUpper(e.Op)
				op = strings.TrimSpace(op)
				switch {
				case (op == "ILIKE" || op == "LIKE") && dbType == "Array(String)":

					variableName := "x"
					lambda := model.NewLambdaExpr([]string{variableName}, model.NewInfixExpr(model.NewLiteral(variableName), op, e.Right.Accept(b).(model.Expr)))
					return model.NewFunction("arrayExists", lambda, e.Left)

				case op == "=":
					return model.NewFunction("has", e.Left, e.Right.Accept(b).(model.Expr))

				default:
					logger.Error().Msgf("Unhandled array infix operation '%s', column '%v' ('%v')", e.Op, column.ColumnName, dbType)
				}
			}
		}

		left := e.Left.Accept(b).(model.Expr)
		right := e.Right.Accept(b).(model.Expr)

		return model.NewInfixExpr(left, e.Op, right)

	}

	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {

		if len(e.Args) > 0 {
			arg := e.Args[0]
			column, ok := arg.(model.ColumnRef)
			if ok {
				dbType := resolver.dbColumnType(column.ColumnName)
				if strings.HasPrefix(dbType, "Array") {
					funcName := e.Name

					ifSuffix := strings.HasSuffix(funcName, "If")
					if ifSuffix {
						funcName = strings.TrimSuffix(funcName, "If")
					}
					orNullSuffix := strings.HasSuffix(funcName, "OrNull")
					if orNullSuffix {
						funcName = strings.TrimSuffix(funcName, "OrNull")
					}

					if aggregateFunctions[strings.ToLower(funcName)] {
						// Use a variant of the function with "Array" suffix:
						// https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-array
						newName := funcName + "Array"
						if orNullSuffix {
							newName = newName + "OrNull"
						}
						if ifSuffix {
							newName = newName + "If"
						}
						e.Name = newName
					} else {
						logger.Error().Msgf("Unhandled array function %s, column %v (%v)", e.Name, column.ColumnName, dbType)
					}
				}
			}
		}

		args := b.VisitChildren(e.Args)
		return model.NewFunction(e.Name, args...)
	}

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		dbType := resolver.dbColumnType(e.ColumnName)
		if strings.HasPrefix(dbType, "Array") {
			logger.Error().Msgf("Unhandled array column ref %v (%v)", e.ColumnName, dbType)
		}
		return e
	}

	return visitor
}

func checkIfGroupingByArrayColumn(selectCommand model.SelectCommand, resolver arrayTypeResolver) bool {

	isArrayColumn := func(e model.Expr) bool {
		columnIsArray := false
		findArrayColumn := model.NewBaseVisitor()

		findArrayColumn.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
			dbType := resolver.dbColumnType(e.ColumnName)
			if strings.HasPrefix(dbType, "Array") {
				columnIsArray = true
			}
			return e
		}

		e.Accept(findArrayColumn)

		return columnIsArray
	}

	visitor := model.NewBaseVisitor()

	var found bool

	visitor.OverrideVisitSelectCommand = func(b *model.BaseExprVisitor, e model.SelectCommand) interface{} {

		for _, expr := range e.GroupBy {

			if isArrayColumn(expr) {
				found = true
			}
		}

		for _, expr := range e.Columns {
			expr.Accept(b)
		}

		if e.FromClause != nil {
			e.FromClause.Accept(b)
		}

		for _, cte := range e.NamedCTEs {
			cte.Accept(b)
		}

		return &e
	}

	selectCommand.Accept(visitor)

	return found
}

func NewArrayJoinVisitor(resolver arrayTypeResolver) model.ExprVisitor {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		dbType := resolver.dbColumnType(e.ColumnName)
		if strings.HasPrefix(dbType, "Array") {
			return model.NewFunction("arrayJoin", e)
		}
		return e
	}

	return visitor
}
