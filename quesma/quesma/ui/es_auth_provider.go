// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package ui

import (
	"github.com/goccy/go-json"
	"github.com/markbates/goth"
	"golang.org/x/oauth2"
)

// ElasticsearchAuthProvider implements the `goth.Provider` for accessing Elasticsearch.
// It is not a real oAuth provider, because essentially it just redirects to Quesma's login page which handles auth via Elasticsearch,
// but in the future this would allow us adding any auth providers we want in a very easy way.
type ElasticsearchAuthProvider struct {
	providerName string
}

func NewElasticsearchAuthProvider() *ElasticsearchAuthProvider {
	return &ElasticsearchAuthProvider{
		providerName: "elasticsearch",
	}
}

type ElasticsearchSession struct{}

func (e ElasticsearchSession) GetAuthURL() (string, error) {
	return loginWithElasticSearch, nil
}

func (e ElasticsearchSession) Marshal() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func (e ElasticsearchSession) Authorize(provider goth.Provider, params goth.Params) (string, error) {
	return "", nil
}

func (e ElasticsearchAuthProvider) Name() string { return e.providerName }

func (e *ElasticsearchAuthProvider) SetName(name string) { e.providerName = name }

func (e ElasticsearchAuthProvider) BeginAuth(state string) (goth.Session, error) {
	return &ElasticsearchSession{}, nil
}

func (e ElasticsearchAuthProvider) UnmarshalSession(s string) (goth.Session, error) {
	return nil, nil
}

func (e ElasticsearchAuthProvider) FetchUser(session goth.Session) (goth.User, error) {
	return goth.User{}, nil
}

func (e ElasticsearchAuthProvider) Debug(b bool) {}

func (e ElasticsearchAuthProvider) RefreshToken(refreshToken string) (*oauth2.Token, error) {
	return nil, nil
}

func (e ElasticsearchAuthProvider) RefreshTokenAvailable() bool { return false }
