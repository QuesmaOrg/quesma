// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package lucene

type token interface{}

type invalidToken struct{}

func newInvalidToken() invalidToken {
	return invalidToken{}
}

type separatorToken struct{}

func newSeparatorToken() separatorToken {
	return separatorToken{}
}

type orToken struct{}

type andToken struct{}

type notToken struct{}

type existsToken struct{}

type leftParenthesisToken struct{}

type rightParenthesisToken struct{}

type rangeToken struct {
	rangeValue
}

func newRangeToken(value rangeValue) rangeToken {
	return rangeToken{value}
}

type fieldNameToken struct {
	fieldName string
}

func newFieldNameToken(fieldName string) fieldNameToken {
	return fieldNameToken{fieldName}
}

type termToken struct {
	term string
}

func newTermToken(term string) termToken {
	return termToken{term}
}

type fuzzyToken struct {
	term     string
	distance int
}

func newFuzzyToken(term string, distance int) fuzzyToken {
	if distance <= 0 {
		distance = 2 // default fuzzy distance
	}
	return fuzzyToken{term: term, distance: distance}
}
