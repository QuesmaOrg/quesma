package elasticsearch

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

type (
	IndexResolver interface {
		Resolve(indexPattern string) (Sources, bool, error)
	}
	indexResolver struct {
		Url        string
		httpClient *http.Client
	}
	Sources struct {
		Indices     []Index      `json:"indices"`
		Aliases     []Alias      `json:"aliases"`
		DataStreams []DataStream `json:"data_streams"`
	}
	Index struct {
		Name       string   `json:"name"`
		Attributes []string `json:"attributes"`
	}
	Alias struct {
		Name    string   `json:"name"`
		Indices []string `json:"indices"`
	}
	DataStream struct {
		Name           string   `json:"name"`
		BackingIndices []string `json:"backing_indices"`
		TimestampField string   `json:"timestamp_field"`
	}
)

func NewIndexResolver(elasticsearchUrl string) IndexResolver {
	return &indexResolver{
		Url:        elasticsearchUrl,
		httpClient: &http.Client{},
	}
}

func (im *indexResolver) Resolve(indexPattern string) (Sources, bool, error) {
	req, _ := http.NewRequest("GET", im.Url+"/_resolve/index/"+indexPattern+"?expand_wildcards=open", bytes.NewBuffer([]byte{}))
	response, err := im.httpClient.Do(req)
	if err != nil {
		return Sources{}, false, err
	}

	if response.StatusCode == http.StatusNotFound {
		return Sources{}, false, nil
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return Sources{}, false, err
	}

	defer response.Body.Close()

	sources := Sources{}
	err = json.Unmarshal(body, &sources)
	if err != nil {
		return Sources{}, false, err
	}

	return sources, true, nil
}
