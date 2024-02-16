package model

import (
	"fmt"
	"mitmproxy/quesma/util"
	"strings"
	"time"
)

type FieldAtIndex = int // for facets/histogram what Cols[i] means

type QueryResultCol struct {
	ColName string // quoted, e.g. `"message"`
	Value   interface{}
}

type QueryResultRow struct {
	Cols []QueryResultCol
}

const (
	ResultColKeyIndex         FieldAtIndex = iota // for facets/histogram Col[0] == Key
	ResultColDocCountIndex                        // for facets/histogram Col[1] == DocCount
	ResultColKeyAsStringIndex                     // for histogram Col[2] == KeyAsString
)

func NewQueryResultCol(colName string, value interface{}) QueryResultCol {
	return QueryResultCol{ColName: colName, Value: value}
}

func (c QueryResultCol) String() string {
	switch c.Value.(type) {
	case string, time.Time:
		return fmt.Sprintf(`"%s": "%v"`, c.ColName, c.Value)
	default:
		return fmt.Sprintf(`"%s": %v`, c.ColName, c.Value)
	}
}

func (r QueryResultRow) String() string {
	str := strings.Builder{}
	str.WriteString(util.Indent(1) + "{\n")
	numCols := len(r.Cols)
	i := 0
	for _, col := range r.Cols {
		str.WriteString(util.Indent(2) + col.String())
		if i < numCols-1 {
			str.WriteString(",")
		}
		str.WriteString("\n")
		i++
	}
	str.WriteString("\n" + util.Indent(1) + "}")
	return str.String()
}
