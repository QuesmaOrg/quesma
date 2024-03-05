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
	switch valueTyped := c.Value.(type) {
	case string, time.Time:
		return fmt.Sprintf(`"%s": "%v"`, c.ColName, c.Value)
	case int, int64, float64, uint64, bool:
		return fmt.Sprintf(`"%s": %v`, c.ColName, c.Value)
	case *string:
		if valueTyped == nil {
			return fmt.Sprintf(`"%s": null`, c.ColName)
		} else {
			return fmt.Sprintf(`"%s": "%v"`, c.ColName, *valueTyped)
		}
	case *time.Time:
		if valueTyped == nil {
			return fmt.Sprintf(`"%s": null`, c.ColName)
		} else {
			return fmt.Sprintf(`"%s": "%v"`, c.ColName, *valueTyped)
		}
	case *int64:
		if valueTyped == nil {
			return fmt.Sprintf(`"%s": null`, c.ColName)
		} else {
			return fmt.Sprintf(`"%s": %v`, c.ColName, *valueTyped)
		}
	case *float64:
		if valueTyped == nil {
			return fmt.Sprintf(`"%s": null`, c.ColName)
		} else {
			return fmt.Sprintf(`"%s": %v`, c.ColName, *valueTyped)
		}
	case *int:
		if valueTyped == nil {
			return fmt.Sprintf(`"%s": null`, c.ColName)
		} else {
			return fmt.Sprintf(`"%s": %v`, c.ColName, *valueTyped)
		}
	case *bool:
		if valueTyped == nil {
			return fmt.Sprintf(`"%s": null`, c.ColName)
		} else {
			return fmt.Sprintf(`"%s": %v`, c.ColName, *valueTyped)
		}
	}

	// TODO Add arrays

	return fmt.Sprintf(`"%s": %v`, c.ColName, c.Value)
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
