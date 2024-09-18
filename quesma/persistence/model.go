// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

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
