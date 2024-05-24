package model

import (
	"fmt"
	"mitmproxy/quesma/queryparser/aexp"
)

type Column struct {
	Alias      string
	Expression aexp.AExp
}

func (c *Column) String() string {

	exprAsString := aexp.RenderSQL(c.Expression)

	if c.Alias == "" {
		return exprAsString
	}

	return fmt.Sprintf("%s AS %s", exprAsString, c.Alias)

}

func Columns(columns ...*Column) []*Column {
	return columns
}

func TODO(format string, args ...interface{}) {

	fmt.Println("TODO: ", fmt.Sprintf(format, args...))
}

const TODOBOTH = true
