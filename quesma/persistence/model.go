// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import "time"

// JSONDatabase is an interface for a database that stores JSON data.
// Treat it as `etcd` equivalent rather than `MongoDB`.
// The main usage is to store our configuration data, like
// - schema
// - user settings
// - statistics, etc
//
// For each case, we should have a separate database.
type JSONDatabase interface {
	List() (keys []string, err error)
	Get(key string) (string, bool, error)
	Put(key string, data string) error
}

// T - type of the data to store, e.g. async_search_storage.AsyncRequestResult
type JSONDatabaseWithEviction interface { // for sure JSON? maybe not only json? check
	Put(row *Sizeable) error
	Get(id string) (*Sizeable, bool)
	Delete(id string)
	DocCount() int
	SizeInBytes() int64
	SizeInBytesLimit() int64
}

type basicDocumentInfo struct {
	id              string
	sizeInBytes     int64
	timestamp       time.Time
	markedAsDeleted bool
}

type Sizeable interface {
	SizeInBytes() int64
}
