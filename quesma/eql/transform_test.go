// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package eql

import (
	"github.com/QuesmaOrg/quesma/quesma/eql/transform"
	"github.com/stretchr/testify/assert"
	"strings"
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
			`(true AND (event.category = 'hostname'))`},

		{`hostname where true and false`,
			`((true AND false) AND (event.category = 'hostname'))`},

		{`hostname where process.name == "init"`,
			`((process.name = 'init') AND (event.category = 'hostname'))`},

		{`hostname where process.pid == 1`,
			`((process.pid = 1) AND (event.category = 'hostname'))`},

		{`any where not true`,
			`(NOT true)`},

		{`any where not (foo == 1)`,
			`(NOT ((foo = 1)))`},

		{`any where not (foo == -1)`,
			`(NOT ((foo = -1)))`},

		{`any where not (foo == 1.2)`,
			`(NOT ((foo = 1.2)))`},

		{`any where process.name in ("naboo", "corusant")`,
			`(process.name IN ('naboo', 'corusant'))`},

		{`any where process.name not in ("naboo", "corusant")`,
			`(process.name NOT IN ('naboo', 'corusant'))`},

		{`hostname where process ==  string(1) `,
			`((process = toString(1)) AND (event.category = 'hostname'))`},

		{`hostname where process ==  string(1) `,
			`((process = toString(1)) AND (event.category = 'hostname'))`},

		{`hostname where process.pid  > 1 + 2`,
			`((process.pid > (1 + 2)) AND (event.category = 'hostname'))`},

		{`any where not process.name : ("naboo", "corusant")`,
			`(NOT ((process.name ILIKE 'naboo') OR (process.name ILIKE 'corusant')))`},

		{`any where process.name == null`,
			`(process.name IS NULL)`},

		{`any where process.name regex ".*"`,
			`match(process.name, '.*')`},

		{"any where process.name == \"quesma.exe\"",
			"(process.name = 'quesma.exe')"},

		{"any where network.protocol == \"http\"", "" +
			"(network.protocol = 'http')"},

		{"any where process.name like  \"FOO*\"         ",
			"(process.name LIKE 'FOO%')"},

		{"any where process.name like~ \"foo*\"         ", "" +
			"(process.name ILIKE 'foo%')"},

		{"any where process.name regex  \"FOO[0-9]\"   ", "" +
			"match(process.name, 'FOO[0-9]')"},

		{"any where process.name regex~ \"foo[0-9]\" ", "" +
			"match(process.name, 'foo[0-9]')"},

		{"any where process.parent.name == \"bar\" and process.name == \"foo\"",
			"((process.parent.name = 'bar') AND (process.name = 'foo'))"},

		{"any where process.name in (\"foo\", \"BAR\", \"BAZ\")",
			"(process.name IN ('foo', 'BAR', 'BAZ'))"},

		{"any where process.name in~ (\"foo\", \"bar\", \"baz\")",
			"(lower(process.name) IN (lower('foo'), lower('bar'), lower('baz')))"},

		{"any where process.name not in (\"foo\", \"BAR\", \"BAZ\")",
			"(process.name NOT IN ('foo', 'BAR', 'BAZ'))"},

		{"any where process.name not in~ (\"foo\", \"bar\", \"baz\")",
			"(lower(process.name) NOT IN (lower('foo'), lower('bar'), lower('baz')))"},

		{"any where process.name : (\"foo\", \"bar\", \"baz\") ",
			"((process.name ILIKE 'foo') OR ((process.name ILIKE 'bar') OR (process.name ILIKE 'baz')))"},

		{"any where process.name like (\"*foo*\", \"bar\", \"baz\") ", "" +
			"((process.name LIKE '%foo%') OR ((process.name LIKE 'bar') OR (process.name LIKE 'baz')))"},

		{"any where process.name like~ (\"*foo*\", \"bar\", \"baz\")",
			"((process.name ILIKE '%foo%') OR ((process.name ILIKE 'bar') OR (process.name ILIKE 'baz')))"},

		{"any where process.name regex  (\"foo.*\", \"bar[0-9]\", \"baz\")    ",
			"(match(process.name, 'foo.*') OR (match(process.name, 'bar[0-9]') OR match(process.name, 'baz')))"},

		// FIXME add case insensitive parameter to regex
		{"any where process.name regex~  (\"foo.*\", \"bar[0-9]\", \"baz\")", "" +
			"TODO (match(process.name, 'foo.*') OR (match(process.name, 'bar[0-9]') OR match(process.name, 'baz')))"},

		{"any where process.pid == ( 4 / process.args_count )",
			"(process.pid = ((4 / process.args_count)))"},

		{"any where process.pid == ( 4.1 / process.args_count) ",
			"(process.pid = ((4.1 / process.args_count)))"},

		{"any where ?user.id != null",
			"TODO (user.id IS NOT NULL)"},
		{"any where ?process.name != null",
			"TODO (process.name IS NOT NULL)"},

		{"any where ?process.name == null", "TODO"},

		//		{"any where foo == \"\"\"\\"ASIS\\"\"\"\"", "TODO"},
		//		{"any where process.name == \"c:\\\\"foo\\"\n\"", "TODO"},

		{"any where process.name : \"foo*\"",
			"(process.name ILIKE 'foo%')"},

		{"any where process.name : \"foo?\"   ",
			"(process.name ILIKE 'foo_')"},

		{"any where process.name like \"FOO?\" ",
			"(process.name LIKE 'FOO_')"},

		{"any where process.name : (\"f*o\", \"ba?\", \"baz\")",
			"((process.name ILIKE 'f%o') OR ((process.name ILIKE 'ba_') OR (process.name ILIKE 'baz')))"},

		{"any where process.name like (\"F*O\", \"BA?\", \"baz\")",
			"((process.name LIKE 'F%O') OR ((process.name LIKE 'BA_') OR (process.name LIKE 'baz')))"},

		{"any where process.pid == add(process.id, 5)", "" +
			"(process.pid = (process.id + 5))"},

		{"any where process.pid == add(process.id, 5.0)", "" +
			"TODO (process.pid = (process.id + 5.0))"},

		{"any where between(file.path, \"System32\\\\\", \".exe\")  == \"\"",
			"TODO not implemented"},

		{"any where cidrMatch(source.address, \"127.0.0.0/16\", \"0.0.0.0/32\")",
			"(isIPAddressInRange(source.address, '127.0.0.0/16') OR isIPAddressInRange(source.address, '0.0.0.0/32'))"},

		{"any where process.name == concat(\"foo\", \"bar\")",
			"(process.name = concat('foo', 'bar'))"},

		{"any where process.pid == divide(512, 2)",
			"(process.pid = (512 / 2))"},

		{"any where endsWith(\"quesma.exe\", \".exe\") ", "" +
			"endsWithUTF8('quesma.exe', '.exe')"},

		{"any where endsWith~(\"Quesma.exe\", \".EXE\") ",
			"endsWithUTF8(lower('Quesma.exe'), lower('.EXE'))"},

		{"any where process.pid == indexOf(url.domain, \".ai\")",
			"(process.pid = position(url.domain, '.ai'))"},

		{"any where process.x == length(\"foo\")",
			"(process.x = length('foo'))"},

		{"any where process.x == modulo(10, 3)", "" +
			"(process.x = (10 % 3))"},

		{"any where process.x == multiply(2, 2)",
			"(process.x = (2 * 2))"},

		{"any where foo == number(\"3.1\") ",
			"(foo = toFloat('3.1'))"},

		{"any where number(\"0xdeadbeef\") = 1 ",
			"TODO"},

		{"any where startsWith(\"quesma.exe\", \"qu\")",
			"startsWithUTF8('quesma.exe', 'qu')"},

		{"any where startsWith~(\"Quesma.exe\", \"qu\")     ", "" +
			"startsWithUTF8(lower('Quesma.exe'), lower('qu'))"},

		{"any where foo == string(2024)",
			"(foo = toString(2024))"},

		{"any where foo == string(true)",
			"(foo = toString(true))"},

		{"any where stringContains(process.command_line, \"quesma\")",
			"hasSubsequence(process.command_line, 'quesma')"},

		{"any where stringContains~(process.command_line, \"Quesma\")",
			"hasSubsequenceCaseInsensitive(process.command_line, 'Quesma')"},

		{"any where process.name == substring(\"start quesma.exe\", 6)",
			"(process.name = substring('start quesma.exe', 6))"},

		{"any where foo == subtract(10, 2)",
			"(foo = (10 - 2))"},

		{"any where 1 == 2",
			"(1 = 2)"},

		{"any where add(1,2) == 2",
			"((1 + 2) = 2)"},

		{"any where  1  == null",
			"(1 IS NULL)"},

		{"any where add(1,null) == 1",
			"((1 + NULL) = 1)"},

		{`any where foo == "\n"`,
			`(foo = '\n')`},

		{`any where foo == "'; delete from table"`,
			`(foo = '\'; delete from table')`},
	}

	for _, tt := range tests {

		t.Run(tt.eql, func(t *testing.T) {

			if strings.HasPrefix(tt.eql, "SKIP") {
				t.Skip("Not yet implemented")
			}

			if strings.HasPrefix(tt.expectedWhereClause, "TODO") {
				t.Skip("Not yet implemented")
			}

			transformer := NewTransformer()
			// paremeter extraction is disabled here for simplicity
			transformer.ExtractParameters = false
			actualWhereClause, _, err := transformer.TransformQuery(tt.eql)

			assert.NotNil(t, actualWhereClause)
			assert.NoError(t, err)
			if err == nil {
				assert.Equal(t, tt.expectedWhereClause, actualWhereClause)
			}
		})
	}
}

func TestTransformWithFieldName(t *testing.T) {

	tests := []struct {
		eql                 string
		expectedWhereClause string
	}{
		{`any where true`,
			`true`}, // TODO add true removal

		{`hostname where true`,
			`(true AND (event::category = 'hostname'))`},

		{`hostname where process.pid == 1`,
			`((process::pid = 1) AND (event::category = 'hostname'))`},

		{`any where not true`,
			`(NOT true)`},

		{`any where not (foo == 1)`,
			`(NOT ((foo = 1)))`},
	}

	for _, tt := range tests {

		t.Run(tt.eql, func(t *testing.T) {

			transformer := NewTransformer()
			transformer.FieldNameTranslator = func(field *transform.Symbol) (*transform.Symbol, error) {
				return transform.NewSymbol(strings.ReplaceAll(field.Name, ".", "::")), nil
			}

			actualWhereClause, parameters, err := transformer.TransformQuery(tt.eql)

			assert.NotNil(t, parameters)
			assert.NotNil(t, actualWhereClause)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedWhereClause, actualWhereClause)
		})
	}
}

func TestErrors(t *testing.T) {

	tests := []struct {
		eql          string
		errorPattern string
	}{
		{`any where ?notexisting == true `,
			`optional fields are not supported`},
		{`any where true | head 1`,
			"unsupported query type"},
		{`any where between(file.path, "System32\\", ".exe")  == ""`,
			`between function is not implemented`},
	}

	for _, tt := range tests {
		t.Run(tt.eql, func(t *testing.T) {

			transformer := NewTransformer()
			_, _, err := transformer.TransformQuery(tt.eql)

			if err == nil {
				t.Error("expected error: ", tt.errorPattern)
				return
			}

			if !strings.Contains(err.Error(), tt.errorPattern) {
				t.Error("expected error: ", tt.errorPattern, " got: ", err)
			}
		})
	}
}
