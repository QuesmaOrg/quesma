package queryprocessor

import (
	"context"
	"mitmproxy/quesma/model"
)

type QueryProcessor struct {
	ctx context.Context
}

func NewQueryProcessor(ctx context.Context) QueryProcessor {
	return QueryProcessor{ctx: ctx}
}

// Returns if row1 and row2 have the same values for the first level fields
func (qp *QueryProcessor) sameGroupByFields(row1, row2 model.QueryResultRow, level int) bool {
	for i := 0; i < level; i++ {
		if row1.Cols[i].ExtractValue(qp.ctx) != row2.Cols[i].ExtractValue(qp.ctx) {
			return false
		}
	}
	return true
}

// Splits ResultSet into buckets, based on the first level fields
// E.g. if level == 0, we split into buckets based on the first field,
// e.g. [row(1, ...), row(1, ...), row(2, ...), row(2, ...), row(3, ...)] -> [[row(1, ...), row(1, ...)], [row(2, ...), row(2, ...)], [row(3, ...)]]
func (qp *QueryProcessor) SplitResultSetIntoBuckets(ResultSet []model.QueryResultRow, level int) [][]model.QueryResultRow {
	if len(ResultSet) == 0 {
		return [][]model.QueryResultRow{{}}
	}

	buckets := [][]model.QueryResultRow{{}}
	curBucket := 0
	lastRow := ResultSet[0]
	for _, row := range ResultSet {
		if qp.sameGroupByFields(row, lastRow, level) {
			buckets[curBucket] = append(buckets[curBucket], row)
		} else {
			curBucket++
			buckets = append(buckets, []model.QueryResultRow{row})
		}
		lastRow = row
	}
	return buckets
}
