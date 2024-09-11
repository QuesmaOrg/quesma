package persistence

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"quesma/quesma/types"
)

type ElasticJSONDatabase struct {
	URL       string
	IndexName string

	httpClient *http.Client
}

func NewElasticJSONDatabase(url, indexName string) *ElasticJSONDatabase {

	return &ElasticJSONDatabase{
		httpClient: &http.Client{},
		URL:        url,
		IndexName:  indexName,
	}
}

func (p *ElasticJSONDatabase) Put(key string, val types.JSON) error {

	elasticsearchURL := fmt.Sprintf("%s/%s/_doc?id=%s", p.URL, p.IndexName, key)

	jsonAsBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", elasticsearchURL, bytes.NewBuffer(jsonAsBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK:
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

func (p *ElasticJSONDatabase) Get(key string) (types.JSON, error) {
	url := fmt.Sprintf("%s/%s/_source/%s", p.URL, p.IndexName, key)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)

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
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	var result types.JSON
	err = json.Unmarshal(jsonAsBytes, &result)
	return result, err

}

func (p *ElasticJSONDatabase) List() ([]string, error) {

	// Define the Elasticsearch endpoint and the index you want to query
	elasticsearchURL := fmt.Sprintf("%s/%s/_search", p.URL, p.IndexName)

	// Build the query to get only document IDs
	query := `{
		"_source": false,
		"size": 100,
		"query": {
			"match_all": {}
		}
	}`

	// Create a new HTTP request
	req, err := http.NewRequest("GET", elasticsearchURL, bytes.NewBuffer([]byte(query)))
	if err != nil {
		log.Fatalf("Error creating HTTP request: %s", err)
	}

	// Set the appropriate headers
	req.Header.Set("Content-Type", "application/json")

	// Use the default HTTP client to execute the request
	client := p.httpClient
	resp, err := client.Do(req)
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
