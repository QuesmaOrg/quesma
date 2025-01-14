// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"context"
	"github.com/QuesmaOrg/quesma/v2/core/tracing"
)

const opaqueIdHeaderKey = "X-Opaque-Id"

type (
	RequestPreprocessor interface {
		PreprocessRequest(ctx context.Context, req *Request) (context.Context, *Request, error)
	}

	ProcessorChain []RequestPreprocessor
)

type (
	TraceIdPreprocessor struct {
		RequestIdGenerator
	}
	RequestIdGenerator func() string
)

func NewTraceIdPreprocessor() TraceIdPreprocessor {
	return TraceIdPreprocessor{RequestIdGenerator: func() string {
		return tracing.GetRequestId()
	}}
}

func (t TraceIdPreprocessor) PreprocessRequest(ctx context.Context, req *Request) (context.Context, *Request, error) {
	rid := t.RequestIdGenerator()
	req.Headers.Add(tracing.RequestIdCtxKey.AsString(), rid)
	ctx = context.WithValue(ctx, tracing.RequestIdCtxKey, rid)
	ctx = context.WithValue(ctx, tracing.RequestPath, req.Path)
	ctx = context.WithValue(ctx, tracing.OpaqueIdCtxKey, req.Headers.Get(opaqueIdHeaderKey))

	return ctx, req, nil
}

var _ RequestPreprocessor = TraceIdPreprocessor{}
