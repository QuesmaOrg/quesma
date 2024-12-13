// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package diag

import (
	"time"
)

type TranslatedSQLQuery struct {
	Query []byte

	PerformedOptimizations []string
	QueryTransformations   []string

	Duration          time.Duration
	RowsReturned      int
	QueryID           string
	ExplainPlan       string
	ExecutionPlanName string
	Error             error
}

type QueryDebugPrimarySource struct {
	Id          string
	QueryResp   []byte
	PrimaryTook time.Duration
}

type QueryDebugSecondarySource struct {
	Id       string
	AsyncId  string
	OpaqueId string

	Path              string
	IncomingQueryBody []byte

	QueryBodyTranslated    []TranslatedSQLQuery
	QueryTranslatedResults []byte
	SecondaryTook          time.Duration

	IsAlternativePlan bool
}

type DebugInfoCollector interface {
	PushPrimaryInfo(qdebugInfo *QueryDebugPrimarySource)
	PushSecondaryInfo(qdebugInfo *QueryDebugSecondarySource)
	RecordRequest(typeName string, took time.Duration, error bool)
}
