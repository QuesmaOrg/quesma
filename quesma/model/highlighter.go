package model

import (
	"mitmproxy/quesma/logger"
	"sort"
	"strings"
)

// Highlighter is a struct that holds information about highlighted fields.
//
// An instance of highlighter is created for each query and is a result of query parsing process,
// so that Fields, PreTags, PostTags are set.
// Once Query is parsed, highlighter visitor is used to traverse the AST and extract tokens
// which should be highlighted.
//
// You can read more in:
//   - https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
//   - https://medium.com/@andre.luiz1987/using-highlighting-elasticsearch-9ccd698f08
type Highlighter struct {
	Tokens map[string]struct{} // tokens represent a 'set' of tokens
	Fields map[string]bool

	PreTags  []string
	PostTags []string
}

// Tokens returns a length-wise sorted list of tokens,
// so that TODO SAY PRECISELY WHY
func (h *Highlighter) GetSortedTokens() []string {
	var tokensList []string
	for token := range h.Tokens {
		tokensList = append(tokensList, token)
	}
	sort.Slice(tokensList, func(i, j int) bool {
		return len(tokensList[i]) > len(tokensList[j])
	})
	return tokensList
}

func (h *Highlighter) ShouldHighlight(columnName string) bool {
	_, ok := h.Fields[columnName]
	return ok
}

// SetTokensToHighlight takes a Select query and extracts tokens that should be highlighted.
func (h *Highlighter) SetTokensToHighlight(selectCmd SelectCommand) {
	highlighterVisitor := NewHighlighter()
	selectCmd.Accept(highlighterVisitor)
	h.Tokens = highlighterVisitor.TokensToHighlight
}

// HighlightValue takes a value and returns the part of it that should be highlighted, wrapped in tags.
//
// E.g. when value is `Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1
// and we search for `Firefo` in Kibana it's going to produce `@kibana-highlighted-field@Firefo@/kibana-highlighted-field@` fr
func (h *Highlighter) HighlightValue(value string) []string {
	// paranoia check for empty tags
	if len(h.PreTags) < 1 && len(h.PostTags) < 1 {
		return []string{}
	}

	type match struct {
		start int
		end   int
	}

	var matches []match

	lowerValue := strings.ToLower(value)
	length := len(lowerValue)

	// find all matches
	for _, token := range h.GetSortedTokens() {
		if token == "" {
			continue
		}
		pos := 0
		for pos < length { // tokens are stored as lowercase
			idx := strings.Index(lowerValue[pos:], token)
			if idx == -1 {
				break
			}
			start := pos + idx
			end := start + len(token)

			matches = append(matches, match{start, end})
			pos = end
		}
	}

	if len(matches) == 0 {
		return []string{}
	}

	// sort matches by start position
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].start < matches[j].start
	})

	var mergedMatches []match

	// merge overlapping matches
	for i := 0; i < len(matches); i++ {
		lastMerged := len(mergedMatches) - 1

		if len(mergedMatches) > 0 && matches[i].start <= mergedMatches[len(mergedMatches)-1].end {
			mergedMatches[lastMerged].end = max(matches[i].end, mergedMatches[lastMerged].end)
		} else {
			mergedMatches = append(mergedMatches, matches[i])
		}
	}

	// populate highlights
	var highlights []string
	for _, m := range mergedMatches {
		highlights = append(highlights, h.PreTags[0]+value[m.start:m.end]+h.PostTags[0])
	}

	return highlights
}

// highlighter is a visitor that traverses the AST and collects tokens that should be highlighted.
type highlighter struct {
	// TokensToHighlight represents a set of tokens that should be highlighted in the query.
	TokensToHighlight map[string]struct{}
}

func NewHighlighter() *highlighter {
	return &highlighter{
		TokensToHighlight: make(map[string]struct{}),
	}
}

func (v *highlighter) VisitColumnRef(e ColumnRef) interface{} {
	return e
}

func (v *highlighter) VisitPrefixExpr(e PrefixExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return NewPrefixExpr(e.Op, exprs)
}

func (v *highlighter) VisitNestedProperty(e NestedProperty) interface{} {
	return NewNestedProperty(e.ColumnRef.Accept(v).(ColumnRef), e.PropertyName)
}

func (v *highlighter) VisitArrayAccess(e ArrayAccess) interface{} {
	return NewArrayAccess(e.ColumnRef.Accept(v).(ColumnRef), e.Index.Accept(v).(Expr))
}

func (v *highlighter) VisitFunction(e FunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return NewFunction(e.Name, exprs...)
}

func (v *highlighter) VisitLiteral(l LiteralExpr) interface{} {
	return l
}

func (v *highlighter) VisitString(e StringExpr) interface{} {
	return e
}

func (v *highlighter) VisitMultiFunction(f MultiFunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range f.Args {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return MultiFunctionExpr{Name: f.Name, Args: exprs}
}

func (v *highlighter) VisitInfix(e InfixExpr) interface{} {
	switch e.Op {
	case "iLIKE", "LIKE", "IN", "=":
		if literal, isLiteral := e.Right.(LiteralExpr); isLiteral {
			switch literalAsString := literal.Value.(type) {
			case string:
				literalAsString = strings.TrimPrefix(literalAsString, "'%")
				literalAsString = strings.TrimPrefix(literalAsString, "%")
				literalAsString = strings.TrimSuffix(literalAsString, "'")
				literalAsString = strings.TrimSuffix(literalAsString, "%")
				v.TokensToHighlight[strings.ToLower(literalAsString)] = struct{}{}
			default:
				logger.Info().Msgf("Value is of an unexpected type: %T\n", literalAsString)
			}
		}
	}
	return NewInfixExpr(e.Left.Accept(v).(Expr), e.Op, e.Right.Accept(v).(Expr))
}

func (v *highlighter) VisitOrderByExpr(e OrderByExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Exprs {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return NewOrderByExpr(exprs, e.Direction)
}

func (v *highlighter) VisitDistinctExpr(e DistinctExpr) interface{} {
	return NewDistinctExpr(e.Expr.Accept(v).(Expr))
}

func (v *highlighter) VisitTableRef(e TableRef) interface{} {
	return e
}

func (v *highlighter) VisitAliasedExpr(e AliasedExpr) interface{} {
	return NewAliasedExpr(e.Expr.Accept(v).(Expr), e.Alias)
}

func (v *highlighter) VisitSelectCommand(c SelectCommand) interface{} {
	var columns, groupBy []Expr
	var orderBy []OrderByExpr
	from := c.FromClause
	where := c.WhereClause
	for _, expr := range c.Columns {
		columns = append(columns, expr.Accept(v).(Expr))
	}
	for _, expr := range c.GroupBy {
		groupBy = append(groupBy, expr.Accept(v).(Expr))
	}
	for _, expr := range c.OrderBy {
		orderBy = append(orderBy, expr.Accept(v).(OrderByExpr))
	}
	if c.FromClause != nil {
		from = c.FromClause.Accept(v).(Expr)
	}
	if c.WhereClause != nil {
		where = c.WhereClause.Accept(v).(Expr)
	}
	return *NewSelectCommand(columns, groupBy, orderBy, from, where, c.Limit, c.SampleLimit, c.IsDistinct)
}

func (v *highlighter) VisitWindowFunction(f WindowFunction) interface{} {
	var args, partitionBy []Expr
	for _, expr := range f.Args {
		args = append(args, expr.Accept(v).(Expr))
	}
	for _, expr := range f.PartitionBy {
		partitionBy = append(partitionBy, expr.Accept(v).(Expr))
	}
	return NewWindowFunction(f.Name, args, partitionBy, f.OrderBy.Accept(v).(OrderByExpr))
}
