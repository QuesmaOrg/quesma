// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/goccy/go-json"
	"io"
	"log"
	"net/http"
)

type ElasticJSONDatabase struct {
	indexName  string
	httpClient *elasticsearch.SimpleClient
}

// This is a wrapper to make document a single field doc.
// We can have documents with more than 1000 fields.
// This is a limitation of Elasticsearch. It's not a real document database.
type Wrapper struct {
	Content string `json:"content"`
}

func NewElasticJSONDatabase(cfg config.ElasticsearchConfiguration, indexName string) *ElasticJSONDatabase {

	return &ElasticJSONDatabase{
		httpClient: elasticsearch.NewSimpleClient(&cfg),
		indexName:  indexName,
	}
}

func (p *ElasticJSONDatabase) refresh() error {

	elasticsearchURL := fmt.Sprintf("%s/_refresh", p.indexName)

	resp, err := p.httpClient.Request(context.Background(), "POST", elasticsearchURL, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
		return nil
	default:

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		logger.Error().Msgf("Failed to flush elastic index: %s", string(body))
		return fmt.Errorf("failed to flush elastic: %v", resp.Status)
	}
}

func (p *ElasticJSONDatabase) Put(key string, data string) error {

	elasticsearchURL := fmt.Sprintf("%s/_update/%s", p.indexName, key)

	w := Wrapper{Content: data}

	updateContent := types.JSON{}
	updateContent["doc"] = w
	updateContent["doc_as_upsert"] = true

	jsonData, err := json.Marshal(updateContent)
	if err != nil {
		return err
	}

	resp, err := p.httpClient.Request(context.Background(), "POST", elasticsearchURL, jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK:

		// We need to flush the index to make sure the data is available for search.
		err = p.refresh()
		if err != nil {
			log.Printf("Failed to flush elastic: %v", err)
			return err
		}

		return nil
	default:
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		fmt.Println(string(respBody))
		return fmt.Errorf("failed to elastic: %v", resp.Status)
	}
}

func (p *ElasticJSONDatabase) Get(key string) (string, bool, error) {
	url := fmt.Sprintf("%s/_source/%s", p.indexName, key)

	resp, err := p.httpClient.Request(context.Background(), "GET", url, nil)
	if err != nil {
		return "", false, err
	}

	defer resp.Body.Close()

	jsonAsBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		break
	case http.StatusNotFound:
		return "", false, nil
	default:
		fmt.Println("failed to get from elastic: ", string(jsonAsBytes))
		return "", false, fmt.Errorf("failed to get from elastic: %v", resp.Status)
	}

	wrapper := Wrapper{}
	err = json.Unmarshal(jsonAsBytes, &wrapper)
	if err != nil {
		return "", false, err
	}

	return wrapper.Content, true, err
}

func (p *ElasticJSONDatabase) List() ([]string, error) {

	// Define the Elasticsearch endpoint and the index you want to query
	elasticsearchURL := fmt.Sprintf("%s/_search", p.indexName)

	// Build the query to get only document IDs

	// We can have more than 10000 indexes.
	// 10000 is the maximum number of documents we can get in a single query. Elasticsearch limitation.
	// TODO:  We need to implement pagination.
	query := `{
		"_source": false,
		"size": 10000, 
		"query": {
			"match_all": {}
		}
	}`

	resp, err := p.httpClient.Request(context.Background(), "GET", elasticsearchURL, []byte(query))

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	jsonAsBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		break
	case http.StatusNotFound:
		return nil, nil
	default:
		fmt.Println("failed to get from elastic: ", string(jsonAsBytes))
		return nil, fmt.Errorf("failed to get from elastic: %v", resp.Status)
	}

	var ids []string
	// Unmarshal the JSON response
	var result map[string]interface{}
	if err := json.Unmarshal(jsonAsBytes, &result); err != nil {
		log.Fatalf("Error parsing the response JSON: %s", err)
	}

	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})

	for _, hit := range hits {
		doc := hit.(map[string]interface{})
		ids = append(ids, doc["_id"].(string))
	}

	return ids, nil
}
