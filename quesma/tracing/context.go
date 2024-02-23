package tracing

import (
	"fmt"
	"sync/atomic"
)

type ContextKey string

const (
	RequestIdCtxKey ContextKey = "RequestId"
	ReasonCtxKey    ContextKey = "Reason"
)

var lastRequestId atomic.Int64

func GetRequestId() string {
	return fmt.Sprintf("%d", lastRequestId.Add(1))
}
