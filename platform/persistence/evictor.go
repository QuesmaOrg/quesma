// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

type EvictorInterface interface {
	Evict(documents []*JSONWithSize, sizeNeeded int64) (bytesEvicted int64)
}

// TODO: Find out how this might work. My old idea doesn't work now,
// as don't remove entire indices, but delete single documents.
// (It turned out consistency was too eventual to rely on it)
// old comment: It's only 1 implementation, which looks well suited for ElasticSearch. It can be implemented differently.
type Evictor struct{}

func (e *Evictor) Evict(documents []*JSONWithSize, sizeNeeded int64) (bytesEvicted int64) {
	panic("implement me (or remove)")
}
