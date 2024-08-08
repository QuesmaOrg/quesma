// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
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
	OpaqueIdCtxKey  ContextKey = "OpaqueId"

	AsyncIdPrefix = "quesma_async_"
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
	return uuid.Must(uuid.NewV7()).String()
}

func GetAsyncId() string {
	return AsyncIdPrefix + uuid.Must(uuid.NewV7()).String()
}

type ContextValues struct {
	RequestId   string
	AsyncId     string
	Reason      string
	RequestPath string
	TraceEnd    bool
	OpaqueId    string
}

func ExtractValues(ctx context.Context) ContextValues {

	str := func(key ContextKey) string {
		if value := ctx.Value(key); value != nil {
			if str, ok := value.(string); ok {
				return str
			}
		}
		return ""
	}

	return ContextValues{
		RequestId:   str(RequestIdCtxKey),
		AsyncId:     str(AsyncIdCtxKey),
		Reason:      str(ReasonCtxKey),
		RequestPath: str(RequestPath),
		OpaqueId:    str(OpaqueIdCtxKey),
	}
}
