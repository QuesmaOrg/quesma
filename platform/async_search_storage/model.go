// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"time"
)

type AsyncRequestResultStorage interface {
	Store(id string, result *AsyncRequestResult)
	Range(func(key string, value *AsyncRequestResult) bool) // ideally I'd like to get rid of this, but not sure if it's possible
	Load(id string) (*AsyncRequestResult, bool)
	Delete(id string)
	Size() int
}

// TODO: maybe merge those 2?
type AsyncQueryContextStorage interface {
	Store(id string, context *AsyncQueryContext)
}

type AsyncRequestResult struct {
	responseBody []byte
	added        time.Time
	isCompressed bool
	err          error
}

func NewAsyncRequestResult(responseBody []byte, err error, added time.Time, isCompressed bool) *AsyncRequestResult {
	return &AsyncRequestResult{responseBody: responseBody, err: err, added: added, isCompressed: isCompressed}
}

func (r *AsyncRequestResult) GetResponseBody() []byte {
	return r.responseBody
}

func (r *AsyncRequestResult) GetErr() error {
	return r.err
}

func (r *AsyncRequestResult) IsCompressed() bool {
	return r.isCompressed
}

type AsyncQueryContext struct {
	id     string
	ctx    context.Context
	cancel context.CancelFunc
	added  time.Time
}

func NewAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, id string) *AsyncQueryContext {
	return &AsyncQueryContext{ctx: ctx, cancel: cancel, added: time.Now(), id: id}
}
