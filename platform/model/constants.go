// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

const (
	SingleTableNamePlaceHolder   = "__quesma_table_name"
	FullTextFieldNamePlaceHolder = "__quesma_fulltext_field_name"
	TimestampFieldName           = "@timestamp"

	DateHourFunction    = "__quesma_date_hour"
	MatchOperator       = "__quesma_match"
	MatchPhraseOperator = "__quesma_match_phrase" // for doris match expression. https://doris.apache.org/docs/sql-manual/basic-element/operators/conditional-operators/full-text-search-operators
)
