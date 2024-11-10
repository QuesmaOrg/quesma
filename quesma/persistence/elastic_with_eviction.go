// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"time"
)

const MAX_DOC_COUNT = 10000                          // TODO: fix/make configurable/idk/etc
const defaultSizeInBytesLimit = int64(1_000_000_000) // 1GB

// so far I serialize entire struct and keep only 1 string in ES
type ElasticDatabaseWithEviction struct {
	ctx                  context.Context
	*ElasticJSONDatabase // maybe remove and copy fields here
	EvictorInterface
	sizeInBytesLimit int64
}

func NewElasticDatabaseWithEviction(cfg config.ElasticsearchConfiguration, indexName string, sizeInBytesLimit int64) *ElasticDatabaseWithEviction {
	return &ElasticDatabaseWithEviction{
		ctx:                 context.Background(),
		ElasticJSONDatabase: NewElasticJSONDatabase(cfg, indexName),
		EvictorInterface:    &Evictor{},
		sizeInBytesLimit:    sizeInBytesLimit,
	}
}

// mutexy? or what
func (db *ElasticDatabaseWithEviction) Put(ctx context.Context, doc *document) bool {
	dbSize, success := db.SizeInBytes()
	if !success {
		return false
	}
	fmt.Println("kk dbg Put() dbSize:", dbSize)
	bytesNeeded := dbSize + doc.SizeInBytes
	if bytesNeeded > db.SizeInBytesLimit() {
		logger.InfoWithCtx(ctx).Msgf("Database is full, need %d bytes more. Evicting documents", bytesNeeded-db.SizeInBytesLimit())
		allDocs, ok := db.getAll()
		if !ok {
			logger.WarnWithCtx(ctx).Msg("Error getting all documents")
			return false
		}
		indexesToEvict, bytesEvicted := db.SelectToEvict(allDocs, bytesNeeded-db.SizeInBytesLimit())
		logger.InfoWithCtx(ctx).Msgf("Evicting %v indexes, %d bytes", indexesToEvict, bytesEvicted)
		db.evict(indexesToEvict)
		bytesNeeded -= bytesEvicted
	}
	if bytesNeeded > db.SizeInBytesLimit() {
		// put document
		return false
	}

	//elasticsearchURL := fmt.Sprintf("%s/_update/%s", db.fullIndexName(), doc.Id)
	elasticsearchURL := fmt.Sprintf("%s/_update/%s", db.indexName, doc.Id)
	fmt.Println("kk dbg Put() elasticsearchURL:", elasticsearchURL)

	updateContent := types.JSON{}
	updateContent["doc"] = doc
	updateContent["doc_as_upsert"] = true

	jsonData, err := json.Marshal(updateContent)
	if err != nil {
		logger.WarnWithCtx(ctx).Msgf("Error marshalling document: %v", err)
		return false
	}

	resp, err := db.httpClient.Request(context.Background(), "POST", elasticsearchURL, jsonData)
	if err != nil {
		logger.WarnWithCtx(ctx).Msgf("Error sending request to elastic: %v", err)
		return false
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK:
		return true
	default:
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.WarnWithCtx(ctx).Msgf("Error reading response body: %v, respBody: %v", err, respBody)
		}
		return false
	}
}

// co zwraca? zrobić switch na oba typy jakie teraz mamy?
func (db *ElasticDatabaseWithEviction) Get(ctx context.Context, id string) (string, bool) { // probably change return type to *Sizeable
	value, success, err := db.ElasticJSONDatabase.Get(id)
	if err != nil {
		logger.WarnWithCtx(ctx).Msgf("Error getting document, id: %s, error: %v", id, err)
		return "", false
	}
	return value, success
}

func (db *ElasticDatabaseWithEviction) Delete(id string) bool {
	// mark as deleted, don't actually delete
	// (single document deletion is hard in ES, it's done by evictor for entire index)

	// TODO: check if doc exists?
	elasticsearchURL := fmt.Sprintf("%s/_update/%s", db.indexName, id)

	updateContent := types.JSON{}
	updateContent["doc"] = types.JSON{"markedAsDeleted": true}
	updateContent["doc_as_upsert"] = true

	jsonData, err := json.Marshal(updateContent)
	if err != nil {
		logger.WarnWithCtx(db.ctx).Msgf("Error marshalling document: %v", err)
		return false
	}

	resp, err := db.httpClient.Request(context.Background(), "POST", elasticsearchURL, jsonData)
	if err != nil {
		logger.WarnWithCtx(db.ctx).Msgf("Error sending request to elastic: %v", err)
		return false
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK:
		return true
	default:
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.WarnWithCtx(db.ctx).Msgf("Error reading response body: %v, respBody: %v", err, respBody)
		}
		return false
	}
}

func (db *ElasticDatabaseWithEviction) DocCount() (count int, success bool) {
	elasticsearchURL := fmt.Sprintf("%s/_search", db.indexName)
	query := `{
		"_source": false,
		"size": 0,
		"track_total_hits": true,
		"query": {
			"term": {
				"markedAsDeleted": {
					"value": false
				}
			}
		}
	}`

	resp, err := db.httpClient.Request(context.Background(), "GET", elasticsearchURL, []byte(query))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	jsonAsBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	fmt.Println("kk dbg DocCount() resp.StatusCode:", resp.StatusCode)

	switch resp.StatusCode {
	case http.StatusOK:
		break
	case http.StatusNoContent, http.StatusNotFound:
		return 0, true
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

	fmt.Println("kk dbg DocCount() result:", result)

	count = int(result["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)) // TODO: add some checks... to prevent panic
	return count, true
}

func (db *ElasticDatabaseWithEviction) SizeInBytes() (sizeInBytes int64, success bool) {
	elasticsearchURL := fmt.Sprintf("%s/_search", db.indexName)
	query := `{
		"_source": ["sizeInBytes"],
		"size": 10000,
		"track_total_hits": true
	}`

	resp, err := db.httpClient.Request(context.Background(), "GET", elasticsearchURL, []byte(query))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	jsonAsBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	fmt.Println("kk dbg SizeInBytes() resp.StatusCode:", resp.StatusCode)

	switch resp.StatusCode {
	case http.StatusOK:
		break
	case http.StatusNoContent, http.StatusNotFound:
		return 0, true
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

	a := make([]int64, 0)
	for _, hit := range result["hits"].(map[string]interface{})["hits"].([]interface{}) {
		b := sizeInBytes
		sizeInBytes += int64(hit.(map[string]interface{})["_source"].(map[string]interface{})["sizeInBytes"].(float64)) // TODO: add checks
		a = append(a, sizeInBytes-b)
	}
	fmt.Println("kk dbg SizeInBytes() sizes in storage:", a)
	return sizeInBytes, true
}

func (db *ElasticDatabaseWithEviction) SizeInBytesLimit() int64 {
	return db.sizeInBytesLimit
}

func (db *ElasticDatabaseWithEviction) getAll() (documents []*document, success bool) {
	elasticsearchURL := fmt.Sprintf("%s*/_search", db.indexName)
	query := `{
		"_source": {
			"excludes": "data"
		},
		"size": 10000,
		"track_total_hits": true
	}`

	resp, err := db.httpClient.Request(context.Background(), "GET", elasticsearchURL, []byte(query))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	jsonAsBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	fmt.Println("kk dbg getAll() resp.StatusCode:", resp.StatusCode)

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

	fmt.Println("kk dbg getAll() documents:")
	for _, hit := range result["hits"].(map[string]interface{})["hits"].([]interface{}) {
		doc := &document{
			Id:          hit.(map[string]interface{})["_id"].(string),
			Index:       hit.(map[string]interface{})["_index"].(string),
			SizeInBytes: int64(hit.(map[string]interface{})["_source"].(map[string]interface{})["sizeInBytes"].(float64)), // TODO: add checks
			//Timestamp:       hit.(map[string]interface{})["_source"].(map[string]interface{})["timestamp"].(time.Time),        // TODO: add checks
			MarkedAsDeleted: hit.(map[string]interface{})["_source"].(map[string]interface{})["markedAsDeleted"].(bool), // TODO: add checks
		}
		fmt.Println(doc)
		documents = append(documents, doc)
	}
	return documents, true
}

func (db *ElasticDatabaseWithEviction) evict(indexes []string) {
	// todo
}

func (db *ElasticDatabaseWithEviction) fullIndexName() string {
	now := time.Now().UTC()
	return fmt.Sprintf("%s-%d-%d-%d-%d-%d-%d", db.indexName, now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
}
