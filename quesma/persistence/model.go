// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import (
	"quesma/quesma/types"
	"time"
)

type (
	// JSONDatabase is an interface for a database that stores JSON data.
	// Treat it as `etcd` equivalent rather than `MongoDB`.
	// The main usage is to store our configuration data, like
	// - schema
	// - user settings
	// - statistics, etc
	//
	// For each case, we should have a separate database.
	JSONDatabase interface {
		List() (keys []string, err error)
		Get(key string) (string, bool, error)
		Put(key string, data string) error
	}
	DatabaseWithEviction interface { // for sure JSON? maybe not only json? check
		Put(doc *JSONWithSize) error
		Get(id string) ([]byte, error)
		Delete(id string) error
		DeleteOld(time.Duration) error
		DocCount() (int, error)
		SizeInBytes() (int64, error)
		SizeInBytesLimit() int64
	}
	JSONWithSize struct {
		types.JSON
		id               string
		SizeInBytesTotal int64
	}
)

func NewJSONWithSize(data types.JSON, id string, sizeInBytesTotal int64) *JSONWithSize {
	return &JSONWithSize{
		JSON:             data,
		id:               id,
		SizeInBytesTotal: sizeInBytesTotal,
	}
}
