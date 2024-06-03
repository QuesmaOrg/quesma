package tracing

import (
	"context"
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

func (c ContextKey) AsString() string {
	return string(c)
}

// NewContextWithRequest creates a new context with the request id and async id from the existing context.
// This is useful for async operations, where we want different cancel functions.
func NewContextWithRequest(existingCtx context.Context) context.Context {
	newContext := context.Background()
	if requestId := existingCtx.Value(RequestIdCtxKey); requestId != nil {
		newContext = context.WithValue(newContext, RequestIdCtxKey, requestId)
	}
	if asyncId := existingCtx.Value(AsyncIdCtxKey); asyncId != nil {
		newContext = context.WithValue(newContext, AsyncIdCtxKey, asyncId)
	}
	return newContext
}

func GetRequestId() string {
	return uuid.New().String()
}
