package aexp

import "strings"

func Function(name string, args ...AExp) FunctionExp {
	return FunctionExp{Name: name, Args: args}
}

func Count(args ...AExp) FunctionExp {
	return Function("count", args...)
}

var Wildcard = LiteralExp{Value: "*"}

// it will render as IS
type symbol string

func Symbol(s string) LiteralExp {
	return Literal(symbol(s))
}

func TableColumn(columnName string) TableColumnExp {
	columnName = strings.TrimSuffix(columnName, ".keyword")
	return TableColumnExp{ColumnName: columnName}
}

func String(value string) StringExp {
	return StringExp{Value: value}
}

func Literal(value any) LiteralExp {
	return LiteralExp{Value: value}
}

func NewComposite(expressions ...AExp) *CompositeExp {
	return &CompositeExp{Expressions: expressions}
}

func Infix(lhs AExp, operator string, rhs AExp) InfixExp {
	return InfixExp{lhs, operator, rhs}
}
