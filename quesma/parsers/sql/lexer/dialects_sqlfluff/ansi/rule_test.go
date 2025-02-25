// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package ansi

import (
	"github.com/QuesmaOrg/quesma/quesma/parsers/sql/lexer/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func FuzzLex(f *testing.F) {
	f.Add("SELECT * FROM tabela")
	f.Add("SELECT id, name, email FROM customers WHERE age > 21")
	f.Add("INSERT INTO products (name, price) VALUES ('Widget', 9.99)")
	f.Add("UPDATE employees SET salary = salary * 1.1 WHERE department = 'Sales'")
	f.Add("DELETE FROM orders WHERE status = 'cancelled'")
	f.Add("SELECT c.name, COUNT(*) as order_count FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name HAVING COUNT(*) > 5")
	f.Add("WITH recursive cte AS (SELECT * FROM tree WHERE parent_id IS NULL UNION ALL SELECT t.* FROM tree t JOIN cte ON t.parent_id = cte.id) SELECT * FROM cte")
	f.Add("SELECT DISTINCT ON (department) name, salary FROM employees ORDER BY department, salary DESC")
	f.Add("CREATE TABLE users (id SERIAL PRIMARY KEY, username VARCHAR(50) NOT NULL UNIQUE)")
	f.Add("ALTER TABLE products ADD COLUMN description TEXT")

	f.Add("SLECT * FORM users")
	f.Add("DELETE FORM WHERE things stuff")

	f.Fuzz(func(t *testing.T, input string) {
		tokens := core.Lex(input, SqlfluffAnsiRules)

		// Basic checks:

		reconstructedInput := ""
		seenError := false

		for _, token := range tokens {
			// Position should never be negative
			if token.Position < 0 {
				t.Errorf("Token position is negative: %d", token.Position)
			}

			// Token raw value should not be empty
			if len(token.RawValue) == 0 {
				t.Error("Token has empty raw value")
			}

			// Position should be within input string bounds
			if token.Position > len(input) {
				t.Errorf("Token position %d exceeds input length %d", token.Position, len(input))
			}

			if token.Type == core.ErrorTokenType {
				seenError = true
			}

			reconstructedInput += token.RawValue
		}

		if !seenError {
			// Tokens should cover the entire input
			assert.Equal(t, input, reconstructedInput)
		}
	})
}

func BenchmarkLex(b *testing.B) {
	testCases := map[string]string{
		"empty":         "",
		"small_query":   "SELECT * FROM tabela",
		"medium_query":  "select * from foo where bar = 1 order by id desc",
		"subquery":      "select * from (select a, b + c as d from table) sub",
		"complex_query": "select 'abc' as foo, json_build_object('a', a,'b', b, 'c', c, 'd', d, 'e', e) as col2col3 from my_table",
		"long_query":    "SELECT t1.column1, t2.column2, t3.column3, SUM(t4.amount) FROM table1 t1 INNER JOIN table2 t2 ON t1.id = t2.id LEFT JOIN table3 t3 ON t2.id = t3.id INNER JOIN table4 t4 ON t3.id = t4.id WHERE t1.date >= '2023-01-01' AND t2.status = 'active' GROUP BY t1.column1, t2.column2, t3.column3 HAVING SUM(t4.amount) > 1000 ORDER BY t1.column1 DESC, t2.column2 ASC LIMIT 100",
		"invalid_query": "SELECT * FORM tabel WERE x = y",
		"garbage":       "!@#$%^&* )( asdf123 ;;; ~~~",
	}

	for name, tc := range testCases {
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				core.Lex(tc, SqlfluffAnsiRules)
			}
		})
	}
}
