package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"quesma/model"
	"testing"
)

func TestApplyWildCard(t *testing.T) {

	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{"test1", []string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{"test2", []string{"*"}, []string{"a", "b", "c"}},
		{"test3", []string{"a", "*"}, []string{"a", "a", "b", "c"}},
		{"test4", []string{"count", "*"}, []string{"count", "a", "b", "c"}},
	}

	table := Table{
		Name: "test",
		Cols: map[string]*Column{
			"a": &Column{Name: "a"},
			"b": &Column{Name: "b"},
			"c": &Column{Name: "c"},
		},
	}

	toSelectColumn := func(cols []string) (res []model.Expr) {
		for _, col := range cols {
			if col == "*" {
				res = append(res, model.NewWildcardExpr)
			} else {
				res = append(res, model.NewColumnRef(col))
			}
		}
		return res
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := &model.Query{
				SelectCommand: model.SelectCommand{
					Columns: toSelectColumn(tt.input),
				},
			}

			table.applyTableSchema(query)

			expectedColumns := toSelectColumn(tt.expected)

			assert.Equal(t, expectedColumns, query.SelectCommand.Columns)
		})
	}
}
