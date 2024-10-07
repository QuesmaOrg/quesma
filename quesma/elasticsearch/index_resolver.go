// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
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

func NormalizePattern(p string) string {
	if p == "_all" {
		return "*"
	}
	return p
}

func (im *indexResolver) Resolve(indexPattern string) (Sources, bool, error) {
	req, err := http.NewRequest("GET", im.Url+"/_resolve/index/"+indexPattern+"?expand_wildcards=open", bytes.NewBuffer([]byte{}))
	if err != nil {
		return Sources{}, false, err
	}
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
