// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import "quesma/quesma/types"

type JSONDatabase interface {
	List() (keys []string, err error)
	Get(key string) (types.JSON, error)
	Put(key string, data types.JSON) error
}
