package eql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTransform(t *testing.T) {

	tests := []struct {
		eql                 string
		expectedWhereClause string
	}{
		{`any where true`,
			`true`}, // TODO add true removal

		{`hostname where true`,
			`(true AND (category.name = 'hostname'))`},

		{`hostname where true and false`,
			`((true AND false) AND (category.name = 'hostname'))`},

		{`hostname where process.name == "init"`,
			`((process.name = 'init') AND (category.name = 'hostname'))`},

		{`hostname where process.pid == 1`,
			`((process.pid = 1) AND (category.name = 'hostname'))`},

		{`any where not true`,
			`(NOT true)`},

		{`any where not (foo == 1)`,
			`(NOT ((foo = 1)))`},

		{`any where process.name in ("naboo", "corusant")`,
			`(process.name IN ('naboo', 'corusant'))`},

		{`any where process.name not in ("naboo", "corusant")`,
			`(process.name NOT IN ('naboo', 'corusant'))`},

		{`hostname where process ==  someFunc(1) `,
			`((process = someFunc(1)) AND (category.name = 'hostname'))`},

		{`hostname where process ==  someFunc~(1) `,
			`((process = someFunc~(1)) AND (category.name = 'hostname'))`},

		{`hostname where process.pid  > 1 + 2`,
			`((process.pid > (1 + 2)) AND (category.name = 'hostname'))`},

		{`any where not process.name : ("naboo", "corusant")`,
			`(NOT ((process.name ILIKE 'naboo') OR (process.name ILIKE 'corusant')))`},

		{`any where process.name == null`,
			`(process.name IS NULL)`},

		{`any where process.name regex ".*"`,
			`match(process.name, '.*')`},
	}

	for _, tt := range tests {

		t.Run(tt.eql, func(t *testing.T) {

			transformer := NewTransformer()

			actualWhereClause, err := transformer.TransformQuery(tt.eql)

			assert.NotNil(t, actualWhereClause)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedWhereClause, actualWhereClause)
		})
	}
}
