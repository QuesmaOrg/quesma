// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

const (
	SingleTableNamePlaceHolder   = "__quesma_table_name"
	FullTextFieldNamePlaceHolder = "__quesma_fulltext_field_name"
	TimestampFieldName           = "@timestamp"

	DateHourFunction                      = "__quesma_date_hour"
	MatchOperator                         = "__quesma_match"
	FromUnixTimestampMsToDateTimeFunction = "__quesma_from_unix_timestamp_ms"
	ToUnixTimestampMs                     = "__quesma_to_unix_timestamp_ms"

	FromUnixTimestampMsToDatetime64Function = "fromUnixTimestamp64Milli"
	FromUnixTimestampMsToDatetimeFunction   = "fromUnixTimestamp"
)
