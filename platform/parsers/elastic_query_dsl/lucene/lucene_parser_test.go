// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package lucene

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/util"
	"testing"
)

func TestTranslatingLuceneQueriesToSQL(t *testing.T) {
	// logger.InitSimpleLoggerForTests()
	defaultFieldNames := []string{"title", "text"}
	var properQueries = []struct {
		query string
		want  string
	}{
		{`title:"The Right Way" AND text:go!!`, `("title" __quesma_match 'The Right Way' AND "text" __quesma_match 'go!!')`},
		{`title:Do it right AND right`, `((("title" __quesma_match 'Do' OR ("title" __quesma_match 'it' OR "text" __quesma_match 'it')) OR ("title" __quesma_match 'right' OR "text" __quesma_match 'right')) AND ("title" __quesma_match 'right' OR "text" __quesma_match 'right'))`},
		{`roam~`, `("title" __quesma_match 'roam' OR "text" __quesma_match 'roam')`},
		{`roam~0.8`, `("title" __quesma_match 'roam' OR "text" __quesma_match 'roam')`},
		{`jakarta^4 apache`, `(("title" __quesma_match 'jakarta' OR "text" __quesma_match 'jakarta') OR ("title" __quesma_match 'apache' OR "text" __quesma_match 'apache'))`},
		{`"jakarta apache"^10`, `("title" __quesma_match 'jakarta apache' OR "text" __quesma_match 'jakarta apache')`},
		{`"jakarta apache"~10`, `("title" __quesma_match 'jakarta apache' OR "text" __quesma_match 'jakarta apache')`},
		{`mod_date:[2002-01-01 TO 2003-02-15]`, `("mod_date" >= '2002-01-01' AND "mod_date" <= '2003-02-15')`}, // 7
		{`mod_date:[2002-01-01 TO 2003-02-15}`, `("mod_date" >= '2002-01-01' AND "mod_date" < '2003-02-15')`},
		{`age:>10`, `"age" > '10'`},
		{`age:>=10`, `"age" >= '10'`},
		{`age:<10`, `"age" < '10'`},
		{`age:<=10.2`, `"age" <= '10.2'`},
		{`age:10.2`, `"age" = 10.2`},
		{`age:-10.2`, `"age" = -10.2`},
		{`age:<-10.2`, `"age" < '-10.2'`},
		{`age:        10.2`, `"age" = 10.2`},
		{`age:  <-10.2`, `"age" < '-10.2'`},
		{`age:  <   -10.2`, `"age" < '-10.2'`},
		{`age:10.2 age2:[12 TO 15] age3:{11 TO *}`, `(("age" = 10.2 OR ("age2" >= '12' AND "age2" <= '15')) OR "age3" > '11')`},
		{`date:{* TO 2012-01-01} another`, `("date" < '2012-01-01' OR ("title" __quesma_match 'another' OR "text" __quesma_match 'another'))`},
		{`date:{2012-01-15 TO *} another`, `("date" > '2012-01-15' OR ("title" __quesma_match 'another' OR "text" __quesma_match 'another'))`},
		{`date:{* TO *}`, `"date" IS NOT NULL`},
		{`title:{Aida TO Carmen]`, `("title" > 'Aida' AND "title" <= 'Carmen')`},
		{`count:[1 TO 5]`, `("count" >= '1' AND "count" <= '5')`}, // 17
		{`"jakarta apache" AND "Apache Lucene"`, `(("title" __quesma_match 'jakarta apache' OR "text" __quesma_match 'jakarta apache') AND ("title" __quesma_match 'Apache Lucene' OR "text" __quesma_match 'Apache Lucene'))`},
		{`NOT status:"jakarta apache"`, `NOT ("status" __quesma_match 'jakarta apache')`},
		{`"jakarta apache" NOT "Apache Lucene"`, `(("title" __quesma_match 'jakarta apache' OR "text" __quesma_match 'jakarta apache') AND NOT (("title" __quesma_match 'Apache Lucene' OR "text" __quesma_match 'Apache Lucene')))`},
		{`(jakarta OR apache) AND website`, `(((("title" __quesma_match 'jakarta' OR "text" __quesma_match 'jakarta')) OR ("title" __quesma_match 'apache' OR "text" __quesma_match 'apache')) AND ("title" __quesma_match 'website' OR "text" __quesma_match 'website'))`},
		{`title:(return "pink panther")`, `("title" __quesma_match 'return' OR "title" __quesma_match 'pink panther')`},
		{`status:(active OR pending) title:(full text search)^2`, `(("status" __quesma_match 'active' OR "status" __quesma_match 'pending') OR (("title" __quesma_match 'full' OR "title" __quesma_match 'text') OR "title" __quesma_match 'search'))`},
		{`status:(active OR NOT (pending AND in-progress)) title:(full text search)^2`, `(("status" __quesma_match 'active' OR NOT (("status" __quesma_match 'pending' AND "status" __quesma_match 'in-progress'))) OR (("title" __quesma_match 'full' OR "title" __quesma_match 'text') OR "title" __quesma_match 'search'))`},
		{`status:(NOT active OR NOT (pending AND in-progress)) title:(full text search)^2`, `((NOT ("status" __quesma_match 'active') OR NOT (("status" __quesma_match 'pending' AND "status" __quesma_match 'in-progress'))) OR (("title" __quesma_match 'full' OR "title" __quesma_match 'text') OR "title" __quesma_match 'search'))`},
		{`status:(active OR (pending AND in-progress)) title:(full text search)^2`, `(("status" __quesma_match 'active' OR ("status" __quesma_match 'pending' AND "status" __quesma_match 'in-progress')) OR (("title" __quesma_match 'full' OR "title" __quesma_match 'text') OR "title" __quesma_match 'search'))`},
		{`status:((a OR (b AND c)) AND d)`, `(("status" __quesma_match 'a' OR ("status" __quesma_match 'b' AND "status" __quesma_match 'c')) AND "status" __quesma_match 'd')`},
		{`title:(return [Aida TO Carmen])`, `("title" __quesma_match 'return' OR ("title" >= 'Aida' AND "title" <= 'Carmen'))`},
		{`host.name:(NOT active OR NOT (pending OR in-progress)) (full text search)^2`, `((((NOT ("host.name" __quesma_match 'active') OR NOT (("host.name" __quesma_match 'pending' OR "host.name" __quesma_match 'in-progress'))) OR (("title" __quesma_match 'full' OR "text" __quesma_match 'full'))) OR ("title" __quesma_match 'text' OR "text" __quesma_match 'text')) OR ("title" __quesma_match 'search' OR "text" __quesma_match 'search'))`},
		{`host.name:(active AND NOT (pending OR in-progress)) hermes nemesis^2`, `((("host.name" __quesma_match 'active' AND NOT (("host.name" __quesma_match 'pending' OR "host.name" __quesma_match 'in-progress'))) OR ("title" __quesma_match 'hermes' OR "text" __quesma_match 'hermes')) OR ("title" __quesma_match 'nemesis' OR "text" __quesma_match 'nemesis'))`},

		// special characters
		{`dajhd \(%&RY#WFDG`, `(("title" __quesma_match 'dajhd' OR "text" __quesma_match 'dajhd') OR ("title" __quesma_match '(\%&RY#WFDG' OR "text" __quesma_match '(\%&RY#WFDG'))`},
		{`x:aaa'bbb`, `"x" __quesma_match 'aaa\'bbb'`},
		{`x:aaa\bbb`, `"x" __quesma_match 'aaa\\bbb'`},
		{`x:aaa*bbb`, `"x" __quesma_match 'aaa%bbb'`},
		{`x:aaa_bbb`, `"x" __quesma_match 'aaa\_bbb'`},
		{`x:aaa%bbb`, `"x" __quesma_match 'aaa\%bbb'`},
		{`x:aaa%\*_bbb`, `"x" __quesma_match 'aaa\%*\_bbb'`},

		// tests for wildcards
		{"%", `("title" __quesma_match '\%' OR "text" __quesma_match '\%')`},
		{`*`, `("title" __quesma_match '%' OR "text" __quesma_match '%')`},
		{`*neme*`, `("title" __quesma_match '%neme%' OR "text" __quesma_match '%neme%')`},
		{`*nem?* abc:ne*`, `(("title" __quesma_match '%nem_%' OR "text" __quesma_match '%nem_%') OR "abc" __quesma_match 'ne%')`},
		{`title:(NOT a* AND NOT (b* OR *))`, `(NOT ("title" __quesma_match 'a%') AND NOT (("title" __quesma_match 'b%' OR "title" __quesma_match '%')))`},
		{`title:abc\*`, `"title" __quesma_match 'abc*'`},
		{`title:abc*\*`, `"title" __quesma_match 'abc%*'`},
		{`ab\+c`, `("title" __quesma_match 'ab+c' OR "text" __quesma_match 'ab+c')`},
		{`!db.str:FAIL`, `NOT ("db.str" __quesma_match 'FAIL')`},
		{`_exists_:title`, `"title" IS NOT NULL`},
		{`!_exists_:title`, `NOT ("title" IS NOT NULL)`},
		{"db.str:*weaver%12*", `"db.str" __quesma_match '%weaver\%12%'`},
		{"(db.str:*weaver*)", `("db.str" __quesma_match '%weaver%')`},
		{"(a.type:*ab* OR a.type:*Ab*)", `(("a.type" __quesma_match '%ab%') OR "a.type" __quesma_match '%Ab%')`},
		{"log:  \"lalala lala la\" AND log: \"troll\"", `("log" __quesma_match 'lalala lala la' AND "log" __quesma_match 'troll')`},
		{"int: 20", `"int" = 20`},
		{`int: "20"`, `"int" __quesma_match '20'`},
	}
	var randomQueriesWithPossiblyIncorrectInput = []struct {
		query string
		want  string
	}{
		{``, `true`},
		{`          `, `true`},
		{`  2 `, `("title" = 2 OR "text" = 2)`},
		{`  2df$ ! `, `(("title" __quesma_match '2df$' OR "text" __quesma_match '2df$') AND NOT (false))`}, // TODO: this should probably just be "false"
		{`title:`, `false`},
		{`title: abc`, `"title" __quesma_match 'abc'`},
		{`title[`, `("title" __quesma_match 'title[' OR "text" __quesma_match 'title[')`},
		{`title[]`, `("title" __quesma_match 'title[]' OR "text" __quesma_match 'title[]')`},
		{`title[ TO ]`, `((("title" __quesma_match 'title[' OR "text" __quesma_match 'title[') OR ("title" __quesma_match 'TO' OR "text" __quesma_match 'TO')) OR ("title" __quesma_match ']' OR "text" __quesma_match ']'))`},
		{`title:[ TO 2]`, `("title" >= '' AND "title" <= '2')`},
		{`  title       `, `("title" __quesma_match 'title' OR "text" __quesma_match 'title')`},
		{`  title : (+a -b c)`, `(("title" __quesma_match '+a' OR "title" __quesma_match '-b') OR "title" __quesma_match 'c')`}, // we don't support '+', '-' operators, but in that case the answer seems good enough + nothing crashes
		{`title:()`, `false`},
		{`() a`, `((false) OR ("title" __quesma_match 'a' OR "text" __quesma_match 'a'))`}, // a bit weird, but '(false)' is OK as I think nothing should match '()'
	}

	currentSchema := schema.Schema{
		Fields: map[schema.FieldName]schema.Field{},
	}

	for i, tt := range append(properQueries, randomQueriesWithPossiblyIncorrectInput...) {
		t.Run(util.PrettyTestName(tt.query, i), func(t *testing.T) {
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
		{query: `title:"The Right Way" AND text:go!!`, mapping: map[string]string{}, want: `("title" __quesma_match 'The Right Way' AND "text" __quesma_match 'go!!')`},
		{query: `age:>10`, mapping: map[string]string{"age": "foo"}, want: `"foo" > '10'`},
	}
	for i, tt := range properQueries {
		t.Run(util.PrettyTestName(tt.query, i), func(t *testing.T) {
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
