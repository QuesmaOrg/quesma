// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

type JSONDatabase interface {
	List() (keys []string, err error)
	Get(key string) (string, bool, error)
	Put(key string, data string) error
}

type VirtualTableColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type VirtualTable struct {
	TableName string                        `json:"name"`
	Columns   map[string]VirtualTableColumn `json:"columns"`
}


