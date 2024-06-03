package quesma

import (
	"context"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/tracing"
)

type (
	RequestPreprocessor interface {
		Applies(req *mux.Request) bool
		PreprocessRequest(ctx context.Context, req *mux.Request) (context.Context, *mux.Request)
	}

	processorChain []RequestPreprocessor
)

type (
	TraceIdPreprocessor struct {
		RequestIdGenerator
	}
	RequestIdGenerator func() string
)

func NewTraceIdPreprocessor() TraceIdPreprocessor {
	return TraceIdPreprocessor{
		RequestIdGenerator: func() string {
			return tracing.GetRequestId()
		},
	}
}

func (t TraceIdPreprocessor) Applies(*mux.Request) bool {
	return true
}

func (t TraceIdPreprocessor) PreprocessRequest(ctx context.Context, req *mux.Request) (context.Context, *mux.Request) {
	rid := t.RequestIdGenerator()
	req.Headers.Add(tracing.RequestIdCtxKey.AsString(), rid)
	ctx = context.WithValue(ctx, tracing.RequestIdCtxKey, rid)
	ctx = context.WithValue(ctx, tracing.RequestPath, req.Path)
	return ctx, req
}

var _ RequestPreprocessor = TraceIdPreprocessor{}
