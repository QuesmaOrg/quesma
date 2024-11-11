// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import "fmt"

type EvictorInterface interface {
	Evict(documents []*JSONWithSize, sizeNeeded int64) (bytesEvicted int64)
}

// It's only 1 implementation, which looks well suited for ElasticSearch.
// It can be implemented differently.
type Evictor struct{}

func (e *Evictor) Evict(documents []*JSONWithSize, sizeNeeded int64) (bytesEvicted int64) {
	if sizeNeeded <= 0 {
		return // check if it's empty array or nil
	}
	fmt.Println("kk dbg SelectToEvict() sizeNeeded:", sizeNeeded)

	return
}
