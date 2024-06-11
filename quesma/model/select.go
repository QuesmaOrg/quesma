package model

// SelectCommand represents a SELECT command of the SQL language
type SelectCommand struct {
	Select     SelectExprs
	From       TableExprs // Can be list of tables but also a subquery
	GroupBy    Exprs
	OrderBy    Exprs
	Where      Exprs
	Limit      Exprs
	IsDistinct bool
}

type Exprs []Expr

type SelectExprs []SelectExpr
type SelectExpr interface{}

type TableExprs []TableExpr
type TableExpr interface {
	tableRefExpr()
}

type TableRefs []TableRef
type TableRef struct {
}

func (*TableRef) tableRefExpr() {}

type SubQuery struct {
	Select SelectStatement
}

func (*SubQuery) tableRefExpr() {}

type SelectStatement interface {
	selectStatement()
}

func (*SelectCommand) selectStatement() {}
