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

type JSONDatabaseWithEviction interface { // for sure JSON? maybe not only json? check
	Put(doc document) bool
	Get(id string) (document, bool)
	Delete(id string)
	DocCount() (int, bool)
	SizeInBytes() (int64, bool)
	SizeInBytesLimit() int64
}

type document struct {
	Id              string    `json:"id"`
	Data            string    `json:"data"`
	Index           string    `json:"index,omitempty"`
	SizeInBytes     int64     `json:"sizeInBytes"`
	Timestamp       time.Time `json:"timestamp"`
	MarkedAsDeleted bool      `json:"markedAsDeleted"`
}

/*
type basicDocumentInfo struct {
	Id               string
	SizeInBytes int64
	Timestamp        time.Time
	MarkedAsDeleted  bool
}

// mb remove or change impl
func (d *basicDocumentInfo) SizeInBytes() int64 {
	return d.SizeInBytesTotal
}
*/
