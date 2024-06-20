package model

import (
	"context"
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/util"
	"reflect"
	"strings"
	"time"
)

type FieldAtIndex = int // for facets/histogram what Cols[i] means

type QueryResultCol struct {
	ColName string // quoted, e.g. `"message"`
	Value   interface{}
}

func NewQueryResultCol(colName string, value interface{}) QueryResultCol {
	return QueryResultCol{ColName: colName, Value: value}
}

type QueryResultRow struct {
	Index string
	Cols  []QueryResultCol
}

func NewQueryResultRowEmpty(index string) QueryResultRow {
	return QueryResultRow{Index: index}
}

const KeyAddedByQuesma = "Quesma_key_JR*#@(DF*GAsFfS!/LI" // created in a way that there shouldn't be a field of this name

const (
	ResultColKeyIndex         FieldAtIndex = iota // for facets/histogram Col[0] == Key
	ResultColDocCountIndex                        // for facets/histogram Col[1] == DocCount
	ResultColKeyAsStringIndex                     // for histogram Col[2] == KeyAsString
)

// String returns the string representation of the column in format `"<colName>": <value>`, properly quoted.
func (c QueryResultCol) String(ctx context.Context) string {
	valueExtracted := c.ExtractValue(ctx)
	if valueExtracted == nil {
		return ""
	}
	switch valueExtracted.(type) {
	case string:
		processed, err := json.Marshal(valueExtracted)
		if err != nil {
			logger.ErrorWithCtx(ctx).Err(err).Msgf("failed to marshal value %v", valueExtracted)
		}
		return fmt.Sprintf(`"%s": %s`, c.ColName, string(processed))
	case time.Time:
		return fmt.Sprintf(`"%s": "%v"`, c.ColName, valueExtracted)
	case int, int64, float64, uint64, bool:
		return fmt.Sprintf(`"%s": %v`, c.ColName, valueExtracted)
	default:
		// Probably good to only use marshaller when necessary, so for arrays/maps,
		// and try to handle simple cases without it
		marshalled, err := json.Marshal(valueExtracted)
		if err != nil {
			logger.ErrorWithCtx(ctx).Err(err).Msgf("failed to marshal value %v", valueExtracted)
		}
		return fmt.Sprintf(`"%s": %v`, c.ColName, string(marshalled))
	}
}

// ExtractValue returns the value of the column. If it is a pointer, it returns the value of the pointer.
// Care: it's untested how it works with '[]type' or '[]*type'.
func (c QueryResultCol) ExtractValue(ctx context.Context) any {
	if c.Value == nil {
		return nil
	}
	v := reflect.ValueOf(c.Value)

	if v.Kind() == reflect.Ptr {
		if v.Elem().Kind() == reflect.Invalid {
			return nil
		}
		return v.Elem().Interface()
	}

	if v.Kind() == reflect.Slice {
		if v.Len() == 0 {
			return nil
		}
		// TODO for now, just return the first element
		// we should probably return a slice of interfaces
		// or string representation of the slice
		return v.Index(0).Interface()
	}

	return c.Value
}

func (r *QueryResultRow) String(ctx context.Context) string {
	str := strings.Builder{}
	str.WriteString(util.Indent(1) + "{\n")
	i := 0
	for _, col := range r.Cols {
		colStr := col.String(ctx)
		if len(colStr) > 0 {
			if i > 0 {
				str.WriteString(",\n")
			}
			str.WriteString(util.Indent(2) + colStr)
			i++
		}
	}
	str.WriteString("\n" + util.Indent(1) + "}")
	return str.String()
}

// Copy returns a deep copy of the row.
func (r *QueryResultRow) Copy() QueryResultRow {
	newCols := make([]QueryResultCol, len(r.Cols))
	copy(newCols, r.Cols)
	return QueryResultRow{Index: r.Index, Cols: newCols}
}

func (r *QueryResultRow) LastColValue() any {
	return r.Cols[len(r.Cols)-1].Value
}
