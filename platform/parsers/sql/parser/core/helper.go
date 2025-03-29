// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package core

func PipeToken() TokenNode {
	return NewTokenNode("|>")
}

func Comma() TokenNode {
	return NewTokenNode(",")
}

func Space() TokenNode {
	return NewTokenNode(" ")
}

// dates

func FormatDateTime() TokenNode {
	return NewTokenNode("formatDateTime")
}

func ToStartOfInterval() TokenNode {
	return NewTokenNode("toStartOfInterval")
}

// functions

func Concat() TokenNode {
	return NewTokenNode("concat")
}

func Regexp() TokenNode {
	return NewTokenNode("REGEXP")
}

func Then() TokenNode {
	return NewTokenNode("THEN")
}

func Plus() TokenNode {
	return NewTokenNode("+")
}

func Interval() TokenNode {
	return NewTokenNode("INTERVAL")
}

func LeftBracket() TokenNode {
	return NewTokenNode("(")
}

func Limit() TokenNode {
	return NewTokenNode("LIMIT")
}

func NewLine() TokenNode {
	return NewTokenNode("\n")
}

func GroupBy() TokenNode {
	return NewTokenNode("GROUP BY")
}

func Aggregate() TokenNode {
	return NewTokenNode("AGGREGATE")
}

func RightBracket() TokenNode {
	return NewTokenNode(")")
}

func Extend() TokenNode {
	return NewTokenNode("EXTEND")
}

func Else() TokenNode {
	return NewTokenNode("ELSE")
}

func QuesmaEnrichValue() TokenNode {
	return NewTokenNode("quesma_enrich.value")
}

func QuesmaEnrich() TokenNode {
	return NewTokenNode("quesma_enrich")
}

func QuesmaEnrichKey() TokenNode {
	return NewTokenNode("quesma_enrich.key")
}

func EnrichType() TokenNode {
	return NewTokenNode("enrich_type")
}

func As() TokenNode {
	return NewTokenNode("AS")
}

func And() TokenNode {
	return NewTokenNode("AND")
}

func On() TokenNode {
	return NewTokenNode("ON")
}

func Select() TokenNode {
	return NewTokenNode("SELECT")
}

func From() TokenNode {
	return NewTokenNode("FROM")
}

func When() TokenNode {
	return NewTokenNode("WHEN")
}

func Equals() TokenNode {
	return NewTokenNode("=")
}

func Case() TokenNode {
	return NewTokenNode("CASE")
}

func LeftJoin() TokenNode {
	return NewTokenNode("LEFT JOIN")
}
