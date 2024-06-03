package tracing

import (
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

func GetRequestId() string {
	return uuid.New().String()
}
