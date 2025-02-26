// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/config"
	"github.com/goccy/go-json"
	"io"
	"net/http"
)

type (
	IndexResolver interface {
		Resolve(indexPattern string) (Sources, bool, error)
	}
	indexResolver struct {
		Url        string
		httpClient *SimpleClient
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

func NewIndexResolver(elasticsearch config.ElasticsearchConfiguration) IndexResolver {
	return &indexResolver{
		Url:        elasticsearch.Url.String(),
		httpClient: NewSimpleClient(&elasticsearch),
	}
}

func NormalizePattern(p string) string {
	if p == "_all" {
		return "*"
	}
	return p
}

func (im *indexResolver) Resolve(indexPattern string) (Sources, bool, error) {
	response, err := im.httpClient.Request(context.Background(), "GET", ResolveIndexPattenPath(indexPattern), []byte{})
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

type EmptyIndexResolver struct {
	Indexes map[string]Sources
}

func NewEmptyIndexResolver() *EmptyIndexResolver {
	return &EmptyIndexResolver{
		Indexes: make(map[string]Sources),
	}
}

func (r *EmptyIndexResolver) Resolve(indexPattern string) (Sources, bool, error) {
	res, ok := r.Indexes[indexPattern]
	return res, ok, nil
}

func ResolveIndexPattenPath(indexPattern string) string {
	return "_resolve/index/" + indexPattern + "?expand_wildcards=open"
}
