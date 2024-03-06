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

// String returns the string representation of the column in format `"<colName>": <value>`, properly quoted.
func (c QueryResultCol) String() string {
	valueExtracted := c.ExtractValue()
	if valueExtracted == nil {
		return fmt.Sprintf(`"%s": null`, c.ColName)
	}
	switch valueTyped := valueExtracted.(type) {
	case string, time.Time:
		return fmt.Sprintf(`"%s": "%v"`, c.ColName, valueTyped)
	default:
		return fmt.Sprintf(`"%s": %v`, c.ColName, valueTyped)
	}
}

// ExtractValue returns the value of the column. If it is a pointer, it returns the value of the pointer.
// Care: it's untested how it works with '[]type' or '[]*type'.
func (c QueryResultCol) ExtractValue() any {
	switch valueTyped := c.Value.(type) {
	case string, time.Time, int, int64, float64, uint64, bool:
		return valueTyped
	case *string:
		if valueTyped == nil {
			return nil
		} else {
			return *valueTyped
		}
	case *time.Time:
		if valueTyped == nil {
			return nil
		} else {
			return *valueTyped
		}
	case *int64:
		if valueTyped == nil {
			return nil
		} else {
			return *valueTyped
		}
	case *float64:
		if valueTyped == nil {
			return nil
		} else {
			return *valueTyped
		}
	case *int:
		if valueTyped == nil {
			return nil
		} else {
			return *valueTyped
		}
	case *bool:
		if valueTyped == nil {
			return nil
		} else {
			return *valueTyped
		}
	}

	// TODO Add arrays

	return c.Value
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
