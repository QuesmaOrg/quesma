// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"quesma/persistence"
	"quesma/quesma/types"
	"time"
)

const EvictionInterval = 15 * time.Minute
const GCInterval = 1 * time.Minute

type AsyncRequestResultStorage interface {
	Store(id string, result *AsyncRequestResult)
	Load(id string) (*AsyncRequestResult, error)
	Delete(id string)
	DocCount() int
	SpaceInUse() int64
	SpaceMaxAvailable() int64

	evict(timeFun func(time.Time) time.Duration)
}

type AsyncQueryContextStorage interface {
	Store(id string, context *AsyncQueryContext)
	evict(timeFun func(time.Time) time.Duration)
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

func (r *AsyncRequestResult) toJSON(id string) *persistence.JSONWithSize {
	json := types.JSON{}
	json["id"] = id
	json["data"] = string(r.responseBody)
	json["sizeInBytes"] = int64(len(r.responseBody)) + int64(len(id)) + 100 // 100 is a rough upper bound estimate of the size of the rest of the fields
	json["added"] = r.added
	return persistence.NewJSONWithSize(json, id, json["sizeInBytes"].(int64))
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

func (c *AsyncQueryContext) toJSON() types.JSON {
	json := types.JSON{}
	json["id"] = c.id
	json["added"] = c.added

	return json
}
