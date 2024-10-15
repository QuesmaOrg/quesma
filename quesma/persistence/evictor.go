// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

type EvictorInterface interface {
	SelectToEvict(documents []*basicDocumentInfo, sizeNeeded int64) (evictThese []*basicDocumentInfo, bytesEvicted int64)
}

// It's only 1 implementation, which looks well suited for ElasticSearch.
// It can be implemented differently.
type Evictor struct{}

func (e *Evictor) SelectToEvict(documents []*basicDocumentInfo, sizeNeeded int64) (evictThese []*basicDocumentInfo, bytesEvicted int64) {
	if sizeNeeded <= 0 {
		return // check if it's empty array or nil
	}

}
