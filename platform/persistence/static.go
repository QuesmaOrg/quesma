// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import (
	"sync"
)

type StaticJSONDatabase struct {
	m    sync.Mutex
	data map[string]string
}

func NewStaticJSONDatabase() *StaticJSONDatabase {
	return &StaticJSONDatabase{
		data: make(map[string]string),
	}
}

func (db *StaticJSONDatabase) List() ([]string, error) {
	db.m.Lock()
	defer db.m.Unlock()

	keys := make([]string, 0, len(db.data))
	for k := range db.data {
		keys = append(keys, k)
	}

	return keys, nil
}

func (db *StaticJSONDatabase) Get(key string) (string, bool, error) {
	db.m.Lock()
	defer db.m.Unlock()

	val, ok := db.data[key]
	return val, ok, nil

}

func (db *StaticJSONDatabase) Put(key string, val string) error {
	db.m.Lock()
	defer db.m.Unlock()

	db.data[key] = val
	return nil
}
