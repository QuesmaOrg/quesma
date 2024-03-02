package clickhouse

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/model"
	"strings"
	"testing"
	"time"
)

// TestProcessHistogramQuery tests if ProcessHistogramQuery returns correct results regardless of DateTime type.
func TestProcessHistogramQuery(t *testing.T) {
	const timestampDividedBy30sec = 1000
	const timestampMsec = timestampDividedBy30sec * 30 * 1000
	const expectedTimestampString = "1970-01-01T08:20:00.000"
	query := model.Query{
		NonSchemaFields: []string{
			"toInt64(toUnixTimestamp64Milli(`timestamp64`)/30000)",
			"toInt64(toUnixTimestamp(`timestamp`)/30.0)",
			"toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)",
		},
	}
	db, mock, _ := sqlmock.New()
	defer db.Close()
	lm := NewLogManagerWithConnection(db, nil)

	mock.ExpectQuery("SELECT " + escapeBrackets(strings.Join(query.NonSchemaFields, ", "))).
		WillReturnRows(sqlmock.NewRows([]string{"key", "doc_count"}).
			AddRow(timestampDividedBy30sec, 0).
			AddRow(timestampDividedBy30sec, 10).
			AddRow(timestampDividedBy30sec, 20))

	rows, err := lm.ProcessHistogramQuery(&query, 30*time.Second)
	assert.NoError(t, err)
	for i, row := range rows {
		assert.Equal(t, int64(timestampMsec), row.Cols[model.ResultColKeyIndex].Value)
		assert.Equal(t, int64(10*i), row.Cols[model.ResultColDocCountIndex].Value)
		assert.Equal(t, expectedTimestampString, row.Cols[model.ResultColKeyAsStringIndex].Value)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal("there were unfulfilled expections:", err)
	}
}

func escapeBrackets(s string) string {
	s = strings.ReplaceAll(s, `(`, `\(`)
	s = strings.ReplaceAll(s, `)`, `\)`)
	s = strings.ReplaceAll(s, `[`, `\[`)
	s = strings.ReplaceAll(s, `]`, `\]`)
	return s
}
