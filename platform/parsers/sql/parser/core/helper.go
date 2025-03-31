// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package core

//
// Feel free to sort it out better, please! I'm not the best in naming/clustering these keywords :(
//

// King Token

func PipeToken() TokenNode {
	return NewTokenNode("|>")
}

// clauses

func Aggregate() TokenNode {
	return NewTokenNode("AGGREGATE")
}

func From() TokenNode {
	return NewTokenNode("FROM")
}

func GroupBy() TokenNode {
	return NewTokenNode("GROUP BY")
}

func LeftJoin() TokenNode {
	return NewTokenNode("LEFT JOIN")
}

func Limit() TokenNode {
	return NewTokenNode("LIMIT")
}

func Select() TokenNode {
	return NewTokenNode("SELECT")
}

// pipe syntax

func EnrichType() TokenNode {
	return NewTokenNode("enrich_type")
}

func Extend() TokenNode {
	return NewTokenNode("EXTEND")
}

func QuesmaEnrich() TokenNode {
	return NewTokenNode("quesma_enrich")
}

func QuesmaEnrichKey() TokenNode {
	return NewTokenNode("quesma_enrich.key")
}

func QuesmaEnrichValue() TokenNode {
	return NewTokenNode("quesma_enrich.value")
}

// special characters

func Comma() TokenNode {
	return NewTokenNode(",")
}

func Equals() TokenNode {
	return NewTokenNode("=")
}

func LeftBracket() TokenNode {
	return NewTokenNode("(")
}

func NewLine() TokenNode {
	return NewTokenNode("\n")
}

func Plus() TokenNode {
	return NewTokenNode("+")
}

func RightBracket() TokenNode {
	return NewTokenNode(")")
}

func Space() TokenNode {
	return NewTokenNode(" ")
}

// operators

func And() TokenNode {
	return NewTokenNode("AND")
}

func Case() TokenNode {
	return NewTokenNode("CASE")
}

func Then() TokenNode {
	return NewTokenNode("THEN")
}

func Else() TokenNode {
	return NewTokenNode("ELSE")
}

func If() TokenNode {
	return NewTokenNode("IF")
}

func Or() TokenNode {
	return NewTokenNode("OR")
}

func When() TokenNode {
	return NewTokenNode("WHEN")
}

// functions

func Concat() TokenNode {
	return NewTokenNode("concat")
}

func Regexp() TokenNode {
	return NewTokenNode("REGEXP")
}

// dates

func FormatDateTime() TokenNode {
	return NewTokenNode("formatDateTime")
}

func Interval() TokenNode {
	return NewTokenNode("INTERVAL")
}

func ToStartOfInterval() TokenNode {
	return NewTokenNode("toStartOfInterval")
}

// ? what type of keyword is this? :D

func As() TokenNode {
	return NewTokenNode("AS")
}

func On() TokenNode {
	return NewTokenNode("ON")
}
