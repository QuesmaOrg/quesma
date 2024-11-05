// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"quesma/logger"
	"quesma/quesma/config"
)

const MAX_DOC_COUNT = 10000 // prototype TODO: fix/make configurable/idk/etc

// so far I serialize entire struct and keep only 1 string in ES
type ElasticDatabaseWithEviction struct {
	ctx                  context.Context
	*ElasticJSONDatabase // maybe remove and copy fields here
	EvictorInterface
	sizeInBytesLimit int64
}

func NewElasticDatabaseWithEviction(ctx context.Context, cfg config.ElasticsearchConfiguration, indexName string, sizeInBytesLimit int64) *ElasticDatabaseWithEviction {
	return &ElasticDatabaseWithEviction{
		ElasticJSONDatabase: NewElasticJSONDatabase(cfg, indexName),
		EvictorInterface:    &Evictor{},
		sizeInBytesLimit:    sizeInBytesLimit,
	}
}

// mutexy? or what
func (db *ElasticDatabaseWithEviction) Put(id string, row Sizeable) bool {
	bytesNeeded := db.SizeInBytes() + row.SizeInBytes()
	if bytesNeeded > db.SizeInBytesLimit() {
		logger.InfoWithCtx(db.ctx).Msg("Database is full, evicting documents")
		//docsToEvict, bytesEvicted := db.SelectToEvict(db.getAll(), bytesNeeded-db.SizeInBytesLimit())
		//db.evict(docsToEvict)
		//bytesNeeded -= bytesEvicted
	}
	if bytesNeeded > db.SizeInBytesLimit() {
		// put document
		return false
	}

	serialized, err := db.serialize(row)
	if err != nil {
		logger.WarnWithCtx(db.ctx).Msg("Error serializing document, id:" + id)
		return false
	}

	err = db.ElasticJSONDatabase.Put(id, serialized)
	if err != nil {
		logger.WarnWithCtx(db.ctx).Msgf("Error putting document, id: %s, error: %v", id, err)
		return false
	}

	return true
}

// co zwraca? zrobić switch na oba typy jakie teraz mamy?
func (db *ElasticDatabaseWithEviction) Get(id string) (string, bool) { // probably change return type to *Sizeable
	value, success, err := db.ElasticJSONDatabase.Get(id)
	if err != nil {
		logger.WarnWithCtx(db.ctx).Msgf("Error getting document, id: %s, error: %v", id, err)
		return "", false
	}
	return value, success
}

func (db *ElasticDatabaseWithEviction) Delete(id string) {
	// mark as deleted, don't actually delete
	// (single document deletion is hard in ES, it's done by evictor for entire index)
}

func (db *ElasticDatabaseWithEviction) DocCount() (count int, success bool) {
	// TODO: add WHERE not_deleted

	// Build the query to get only document IDs
	elasticsearchURL := fmt.Sprintf("%s/_search", db.indexName)
	query := `{
		"_source": false,
		"size": 0,
		"track_total_hits": true
	}`

	resp, err := db.httpClient.Request(context.Background(), "GET", elasticsearchURL, []byte(query))
	defer resp.Body.Close()
	if err != nil {
		return
	}

	jsonAsBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	switch resp.StatusCode {
	case http.StatusOK:
		break
	default:
		logger.WarnWithCtx(db.ctx).Msgf("failed to get from elastic: %s, response status code: %v", string(jsonAsBytes), resp.StatusCode)
		return
	}

	// Unmarshal the JSON response
	var result map[string]interface{}
	if err = json.Unmarshal(jsonAsBytes, &result); err != nil {
		logger.WarnWithCtx(db.ctx).Msgf("Error parsing the response JSON: %s", err)
		return
	}

	count = int(result["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)) // TODO: add some checks... to prevent panic
	return count, true
}

func (db *ElasticDatabaseWithEviction) SizeInBytes() (sizeInBytes int64, success bool) {
	elasticsearchURL := fmt.Sprintf("%s/_search", db.indexName)

	// Build the query to get only document IDs
	query := fmt.Sprintf(`{"_source": false, "size": %d}`, MAX_DOC_COUNT)
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

func (db *ElasticDatabaseWithEviction) serialize(row Sizeable) (serialized string, err error) {
	var b bytes.Buffer

	enc := gob.NewEncoder(&b) // maybe create 1 encoder forever
	if err = enc.Encode(row); err != nil {
		fmt.Println("Error encoding struct:", err)
		return
	}

	return b.String(), nil
}
