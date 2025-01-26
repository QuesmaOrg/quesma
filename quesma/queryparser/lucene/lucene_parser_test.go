// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package lucene

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"strconv"
	"testing"
)

func TestTranslatingLuceneQueriesToSQL(t *testing.T) {
	// logger.InitSimpleLoggerForTests()
	defaultFieldNames := []string{"title", "text"}
	var properQueries = []struct {
		query string
		want  string
	}{
		{`title:"The Right Way" AND text:go!!`, `("title" = 'The Right Way' AND "text" = 'go!!')`},
		{`title:Do it right AND right`, `((("title" = 'Do' OR ("title" = 'it' OR "text" = 'it')) OR ("title" = 'right' OR "text" = 'right')) AND ("title" = 'right' OR "text" = 'right'))`},
		{`roam~`, `("title" = 'roam' OR "text" = 'roam')`},
		{`roam~0.8`, `("title" = 'roam' OR "text" = 'roam')`},
		{`jakarta^4 apache`, `(("title" = 'jakarta' OR "text" = 'jakarta') OR ("title" = 'apache' OR "text" = 'apache'))`},
		{`"jakarta apache"^10`, `("title" = 'jakarta apache' OR "text" = 'jakarta apache')`},
		{`"jakarta apache"~10`, `("title" = 'jakarta apache' OR "text" = 'jakarta apache')`},
		{`mod_date:[2002-01-01 TO 2003-02-15]`, `("mod_date" >= '2002-01-01' AND "mod_date" <= '2003-02-15')`}, // 7
		{`mod_date:[2002-01-01 TO 2003-02-15}`, `("mod_date" >= '2002-01-01' AND "mod_date" < '2003-02-15')`},
		{`age:>10`, `"age" > '10'`},
		{`age:>=10`, `"age" >= '10'`},
		{`age:<10`, `"age" < '10'`},
		{`age:<=10.2`, `"age" <= '10.2'`},
		{`age:10.2`, `"age" = '10.2'`},
		{`age:-10.2`, `"age" = '-10.2'`},
		{`age:<-10.2`, `"age" < '-10.2'`},
		{`age:        10.2`, `"age" = '10.2'`},
		{`age:  <-10.2`, `"age" < '-10.2'`},
		{`age:  <   -10.2`, `"age" < '-10.2'`},
		{`age:10.2 age2:[12 TO 15] age3:{11 TO *}`, `(("age" = '10.2' OR ("age2" >= '12' AND "age2" <= '15')) OR "age3" > '11')`},
		{`date:{* TO 2012-01-01} another`, `("date" < '2012-01-01' OR ("title" = 'another' OR "text" = 'another'))`},
		{`date:{2012-01-15 TO *} another`, `("date" > '2012-01-15' OR ("title" = 'another' OR "text" = 'another'))`},
		{`date:{* TO *}`, `"date" IS NOT NULL`},
		{`title:{Aida TO Carmen]`, `("title" > 'Aida' AND "title" <= 'Carmen')`},
		{`count:[1 TO 5]`, `("count" >= '1' AND "count" <= '5')`}, // 17
		{`"jakarta apache" AND "Apache Lucene"`, `(("title" = 'jakarta apache' OR "text" = 'jakarta apache') AND ("title" = 'Apache Lucene' OR "text" = 'Apache Lucene'))`},
		{`NOT status:"jakarta apache"`, `NOT ("status" = 'jakarta apache')`},
		{`"jakarta apache" NOT "Apache Lucene"`, `(("title" = 'jakarta apache' OR "text" = 'jakarta apache') AND NOT (("title" = 'Apache Lucene' OR "text" = 'Apache Lucene')))`},
		{`(jakarta OR apache) AND website`, `(((("title" = 'jakarta' OR "text" = 'jakarta')) OR ("title" = 'apache' OR "text" = 'apache')) AND ("title" = 'website' OR "text" = 'website'))`},
		{`title:(return "pink panther")`, `("title" = 'return' OR "title" = 'pink panther')`},
		{`status:(active OR pending) title:(full text search)^2`, `(("status" = 'active' OR "status" = 'pending') OR (("title" = 'full' OR "title" = 'text') OR "title" = 'search'))`},
		{`status:(active OR NOT (pending AND in-progress)) title:(full text search)^2`, `(("status" = 'active' OR NOT (("status" = 'pending' AND "status" = 'in-progress'))) OR (("title" = 'full' OR "title" = 'text') OR "title" = 'search'))`},
		{`status:(NOT active OR NOT (pending AND in-progress)) title:(full text search)^2`, `((NOT ("status" = 'active') OR NOT (("status" = 'pending' AND "status" = 'in-progress'))) OR (("title" = 'full' OR "title" = 'text') OR "title" = 'search'))`},
		{`status:(active OR (pending AND in-progress)) title:(full text search)^2`, `(("status" = 'active' OR ("status" = 'pending' AND "status" = 'in-progress')) OR (("title" = 'full' OR "title" = 'text') OR "title" = 'search'))`},
		{`status:((a OR (b AND c)) AND d)`, `(("status" = 'a' OR ("status" = 'b' AND "status" = 'c')) AND "status" = 'd')`},
		{`title:(return [Aida TO Carmen])`, `("title" = 'return' OR ("title" >= 'Aida' AND "title" <= 'Carmen'))`},
		{`host.name:(NOT active OR NOT (pending OR in-progress)) (full text search)^2`, `((((NOT ("host.name" = 'active') OR NOT (("host.name" = 'pending' OR "host.name" = 'in-progress'))) OR (("title" = 'full' OR "text" = 'full'))) OR ("title" = 'text' OR "text" = 'text')) OR ("title" = 'search' OR "text" = 'search'))`},
		{`host.name:(active AND NOT (pending OR in-progress)) hermes nemesis^2`, `((("host.name" = 'active' AND NOT (("host.name" = 'pending' OR "host.name" = 'in-progress'))) OR ("title" = 'hermes' OR "text" = 'hermes')) OR ("title" = 'nemesis' OR "text" = 'nemesis'))`},
		{`dajhd \(%&RY#WFDG`, `(("title" = 'dajhd' OR "text" = 'dajhd') OR ("title" = '(%&RY#WFDG' OR "text" = '(%&RY#WFDG'))`},
		// tests for wildcards
		{`*`, `("title" ILIKE '%' OR "text" ILIKE '%')`},
		{`*neme*`, `("title" ILIKE '%neme%' OR "text" ILIKE '%neme%')`},
		{`*nem?* abc:ne*`, `(("title" ILIKE '%nem_%' OR "text" ILIKE '%nem_%') OR "abc" ILIKE 'ne%')`},
		{`title:(NOT a* AND NOT (b* OR *))`, `(NOT ("title" ILIKE 'a%') AND NOT (("title" ILIKE 'b%' OR "title" ILIKE '%')))`},
		{`title:abc\*`, `"title" = 'abc*'`},
		{`title:abc*\*`, `"title" ILIKE 'abc%*'`},
		{`ab\+c`, `("title" = 'ab+c' OR "text" = 'ab+c')`},
		{`!db.str:FAIL`, `NOT ("db.str" = 'FAIL')`},
		{`_exists_:title`, `"title" IS NOT NULL`},
		{`!_exists_:title`, `NOT ("title" IS NOT NULL)`},
		{"db.str:*weaver*", `"db.str" ILIKE '%weaver%'`},
		{"(db.str:*weaver*)", `("db.str" ILIKE '%weaver%')`},
		{"(a.type:*ab* OR a.type:*Ab*)", `(("a.type" ILIKE '%ab%') OR "a.type" ILIKE '%Ab%')`},
		{"log:  \"lalala lala la\" AND log: \"troll\"", `("log" iLIKE '%lalala lala la%' AND "log" iLIKE '%troll%')`},
	}
	var randomQueriesWithPossiblyIncorrectInput = []struct {
		query string
		want  string
	}{
		{``, `true`},
		{`          `, `true`},
		{`  2 `, `("title" = '2' OR "text" = '2')`},
		{`  2df$ ! `, `(("title" = '2df$' OR "text" = '2df$') AND NOT (false))`}, // TODO: this should probably just be "false"
		{`title:`, `false`},
		{`title: abc`, `"title" = 'abc'`},
		{`title[`, `("title" = 'title[' OR "text" = 'title[')`},
		{`title[]`, `("title" = 'title[]' OR "text" = 'title[]')`},
		{`title[ TO ]`, `((("title" = 'title[' OR "text" = 'title[') OR ("title" = 'TO' OR "text" = 'TO')) OR ("title" = ']' OR "text" = ']'))`},
		{`title:[ TO 2]`, `("title" >= '' AND "title" <= '2')`},
		{`  title       `, `("title" = 'title' OR "text" = 'title')`},
		{`  title : (+a -b c)`, `(("title" = '+a' OR "title" = '-b') OR "title" = 'c')`}, // we don't support '+', '-' operators, but in that case the answer seems good enough + nothing crashes
		{`title:()`, `false`},
		{`() a`, `((false) OR ("title" = 'a' OR "text" = 'a'))`}, // a bit weird, but '(false)' is OK as I think nothing should match '()'
	}

	currentSchema := schema.Schema{
		Fields: map[schema.FieldName]schema.Field{},
	}

	for i, tt := range append(properQueries, randomQueriesWithPossiblyIncorrectInput...) {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			parser := newLuceneParser(context.Background(), defaultFieldNames, currentSchema)
			got := model.AsString(parser.translateToSQL(tt.query))
			if got != tt.want {
				t.Errorf("\ngot  [%q]\nwant [%q]", got, tt.want)
			}
		})
	}
}

func TestResolvePropertyNamesWhenTranslatingToSQL(t *testing.T) {

	defaultFieldNames := []string{"title", "text"}
	var properQueries = []struct {
		query   string
		mapping map[string]string
		want    string
	}{
		{query: `title:"The Right Way" AND text:go!!`, mapping: map[string]string{}, want: `("title" = 'The Right Way' AND "text" = 'go!!')`},
		{query: `age:>10`, mapping: map[string]string{"age": "foo"}, want: `"foo" > '10'`},
	}
	for i, tt := range properQueries {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			fields := make(map[schema.FieldName]schema.Field)

			for k, v := range tt.mapping {
				fields[schema.FieldName(k)] = schema.Field{PropertyName: schema.FieldName(k), InternalPropertyName: schema.FieldName(v)}
			}

			currentSchema := schema.Schema{Fields: fields}

			parser := newLuceneParser(context.Background(), defaultFieldNames, currentSchema)
			got := model.AsString(parser.translateToSQL(tt.query))
			if got != tt.want {
				t.Errorf("\ngot  [%q]\nwant [%q]", got, tt.want)
			}
		})
	}
}
