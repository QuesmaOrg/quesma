// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/k0kubun/pp"
	"io"
	"math"
	"net/http"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"time"
)

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

func (db *ElasticDatabaseWithEviction) Put(document *JSONWithSize) error {
	pp.Println(db)
	dbSize, err := db.SizeInBytes()
	if err != nil {
		return err
	}
	fmt.Println("kk dbg Put() dbSize:", dbSize)
	bytesNeeded := dbSize + document.SizeInBytesTotal // improve
	if bytesNeeded > db.SizeInBytesLimit() {
		logger.Info().Msgf("elastic database: is full, need %d bytes more. Evicting documents", bytesNeeded-db.SizeInBytesLimit())
		allDocs, err := db.getAll()
		if err != nil {
			return err
		}
		bytesEvicted := db.Evict(allDocs, bytesNeeded-db.SizeInBytesLimit())
		logger.Info().Msgf("elastic database: evicted %d bytes", bytesEvicted)
		bytesNeeded -= bytesEvicted
	}
	if bytesNeeded > db.SizeInBytesLimit() {
		return errors.New("elastic database: is full, cannot put document")
	}

	elasticsearchURL := fmt.Sprintf("%s/_update/%s", db.indexName, document.id)
	fmt.Println("kk dbg Put() elasticsearchURL:", elasticsearchURL)

	updateContent := types.JSON{}
	updateContent["doc"] = document.JSON
	updateContent["doc_as_upsert"] = true

	jsonData, err := json.Marshal(updateContent)
	if err != nil {
		return err
	}

	resp, err := db.httpClient.DoRequestCheckResponseStatusOK(context.Background(), http.MethodPost, elasticsearchURL, jsonData)
	fmt.Println("kk dbg Put() resp:", resp, "err:", err)
	if err != nil && (resp == nil || resp.StatusCode != http.StatusCreated) {
		return err
	}
	return nil
}

// co zwraca? zrobić switch na oba typy jakie teraz mamy?
func (db *ElasticDatabaseWithEviction) Get(id string) ([]byte, error) { // probably change return type to *Sizeable
	elasticsearchURL := fmt.Sprintf("%s/_source/%s", db.indexName, id)
	resp, err := db.httpClient.DoRequestCheckResponseStatusOK(context.Background(), http.MethodGet, elasticsearchURL, nil)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (db *ElasticDatabaseWithEviction) Delete(id string) error {
	// mark as deleted, don't actually delete
	// (single document deletion is hard in ES, it's done by evictor for entire index)

	// TODO: check if doc exists?
	elasticsearchURL := fmt.Sprintf("%s/_doc/%s", db.indexName, id)
	resp, err := db.httpClient.DoRequestCheckResponseStatusOK(context.Background(), http.MethodDelete, elasticsearchURL, nil)
	if err != nil && (resp == nil || resp.StatusCode != http.StatusCreated) {
		return err
	}
	return nil
}

func (db *ElasticDatabaseWithEviction) DeleteOld(deleteOlderThan time.Duration) (err error) {
	if deleteOlderThan < 1*time.Second {
		deleteOlderThan = 1 * time.Second
	}

	rangeStr := fmt.Sprintf("now-%dm", int(math.Floor(deleteOlderThan.Minutes())))
	if deleteOlderThan < 5*time.Minute {
		rangeStr = fmt.Sprintf("now-%ds", int(math.Floor(deleteOlderThan.Seconds())))
	}

	elasticsearchURL := fmt.Sprintf("%s/_delete_by_query", db.indexName)
	query := fmt.Sprintf(`{
		"query": {
			"range": {
				"added": {
					"lte": "%s"
				}
			}
		}
	}`, rangeStr)

	fmt.Println(query)

	var resp *http.Response
	resp, err = db.httpClient.DoRequestCheckResponseStatusOK(context.Background(), http.MethodPost, elasticsearchURL, []byte(query))
	fmt.Println("kk dbg DocCount() resp:", resp, "err:", err, "elastic url:", elasticsearchURL)
	return err
}

func (db *ElasticDatabaseWithEviction) DocCount() (docCount int, err error) {
	elasticsearchURL := fmt.Sprintf("%s/_search", db.indexName)
	query := `{
		"_source": false,
		"size": 0,
		"track_total_hits": true
	}`

	var resp *http.Response
	resp, err = db.httpClient.DoRequestCheckResponseStatusOK(context.Background(), http.MethodGet, elasticsearchURL, []byte(query))
	fmt.Println("kk dbg DocCount() resp:", resp, "err:", err, "elastic url:", elasticsearchURL)
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound) {
			return 0, nil
		}
		return -1, err
	}

	var jsonAsBytes []byte
	jsonAsBytes, err = io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	// Unmarshal the JSON response
	var result map[string]interface{}
	if err = json.Unmarshal(jsonAsBytes, &result); err != nil {
		return
	}

	fmt.Println("kk dbg DocCount() result:", result)

	return int(result["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)), nil // TODO: add some checks... to prevent panic
}

func (db *ElasticDatabaseWithEviction) SizeInBytes() (sizeInBytes int64, err error) {
	elasticsearchURL := fmt.Sprintf("%s/_search", db.indexName)
	query := `{
		"_source": ["sizeInBytes"],
		"size": 10000,
		"track_total_hits": true
	}`

	var resp *http.Response
	resp, err = db.httpClient.DoRequestCheckResponseStatusOK(context.Background(), http.MethodGet, elasticsearchURL, []byte(query))
	fmt.Println("kk dbg SizeInBytes() err:", err, "\nresp:", resp)
	if err != nil {
		if resp != nil && resp.StatusCode == 404 {
			return 0, nil
		}
		return
	}
	defer resp.Body.Close() // add everywhere

	var jsonAsBytes []byte
	jsonAsBytes, err = io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	fmt.Println("kk dbg SizeInBytes() resp.StatusCode:", resp.StatusCode)

	// Unmarshal the JSON response
	var result map[string]interface{}
	if err = json.Unmarshal(jsonAsBytes, &result); err != nil {
		return
	}

	a := make([]int64, 0)
	for _, hit := range result["hits"].(map[string]interface{})["hits"].([]interface{}) {
		pp.Println("hit:", hit)
		b := sizeInBytes
		sizeInBytes += int64(hit.(map[string]interface{})["_source"].(map[string]interface{})["sizeInBytes"].(float64)) // TODO: add checks
		a = append(a, sizeInBytes-b)
	}
	fmt.Println("kk dbg SizeInBytes() sizes in storage:", a)
	return sizeInBytes, nil
}

func (db *ElasticDatabaseWithEviction) SizeInBytesLimit() int64 {
	return db.sizeInBytesLimit
}

func (db *ElasticDatabaseWithEviction) getAll() (documents []*JSONWithSize, err error) {
	_ = fmt.Sprintf("%s*/_search", db.indexName)
	_ = `{
		"_source": {
			"excludes": "data"
		},
		"size": 10000,
		"track_total_hits": true
	}`
	/*
		db.httpClient.

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

	*/
	return documents, nil
}

func (db *ElasticDatabaseWithEviction) fullIndexName() string {
	now := time.Now().UTC()
	return fmt.Sprintf("%s-%d-%d-%d-%d-%d-%d", db.indexName, now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
}
