// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import "quesma/quesma/config"

type ElasticDatabaseWithEviction struct {
	*ElasticJSONDatabase // maybe remove and copy fields here
	EvictorInterface
	sizeInBytesLimit int64
}

func NewElasticDatabaseWithEviction(cfg config.ElasticsearchConfiguration,
	indexName string, sizeInBytesLimit int64) *ElasticDatabaseWithEviction {

	return &ElasticDatabaseWithEviction{
		ElasticJSONDatabase: NewElasticJSONDatabase(cfg, indexName),
		EvictorInterface:    &Evictor{},
		sizeInBytesLimit:    sizeInBytesLimit,
	}
}

// mutexy? or what
func (db *ElasticDatabaseWithEviction) Put(row Sizeable) bool {
	bytesNeeded := db.SizeInBytes() + row.SizeInBytes()
	if bytesNeeded > db.SizeInBytesLimit() {
		docsToEvict, bytesEvicted := db.SelectToEvict(db.getAll(), bytesNeeded-db.SizeInBytesLimit())
		db.evict(docsToEvict)
		bytesNeeded -= bytesEvicted
	}

	if bytesNeeded <= db.SizeInBytesLimit() {
		// put document
		return true
	}
	return false
}

func (db *ElasticDatabaseWithEviction) Get(id string) (*Sizeable, bool) {
	// either use ElasticJSONDatabase.Get or implement own
	// doesn't matter
	return nil, false
}

func (db *ElasticDatabaseWithEviction) Delete(id string) {
	// mark as deleted, don't actually delete
	// (single document deletion is hard in ES, it's done by evictor for entire index)
}

func (db *ElasticDatabaseWithEviction) DocCount() int {
	// send count() query to ES
	return 0
}

func (db *ElasticDatabaseWithEviction) SizeInBytes() int64 {
	return 0
}

func (db *ElasticDatabaseWithEviction) SizeInBytesLimit() int64 {
	return db.sizeInBytesLimit
}

func (db *ElasticDatabaseWithEviction) getAll() *basicDocumentInfo {
	// send query
	return nil
}

func (db *ElasticDatabaseWithEviction) evict(documents []*basicDocumentInfo) {

}
