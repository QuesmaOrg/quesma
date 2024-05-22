package tracing

import (
	"context"
	"fmt"
	"github.com/google/uuid"
)

type ContextKey string

const (
	RequestIdCtxKey ContextKey = "RequestId"
	ReasonCtxKey    ContextKey = "Reason"
	RequestPath     ContextKey = "RequestPath"
	AsyncIdCtxKey   ContextKey = "AsyncId"
	TraceEndCtxKey  ContextKey = "TraceEnd"
)

func GetRequestId() string {
	return uuid.New().String()
}

func WithReason(ctx context.Context, reason string) context.Context {

	currentReason := ctx.Value(ReasonCtxKey)

	if currentReason != nil {
		reason = fmt.Sprintf("%s: %s", currentReason, reason)
	}

	return context.WithValue(ctx, ReasonCtxKey, reason)
}
