// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package feature

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewUnsupportedFeature_index(t *testing.T) {

	tests := []struct {
		path     string
		opaqueId string
		isLogged bool
	}{
		{"/foo/_search", "", true},
		{"/foo/_new_feature", "", true},
		{"/bar/_search", "", false},

		{"/foo/_search/template", "", true},
		{"/_scripts/foo", "", true},

		{"/logs-elastic_agent-*/_search", "unknownId;kibana:task%20manager:run%20Fleet-Usage-Sender:Fleet-Usage-Sender-1.1.3", false},
		{"/foo/_search", "unknownId;kibana:task%20manager:run%20Fleet-Usage-Sender:Fleet-Usage-Sender-1.1.3", false},
	}

	cfg := config.QuesmaConfiguration{}
	cfg.IndexConfig = map[string]config.IndexConfiguration{
		"foo": {},
	}

	ctx := context.Background()

	indexNameResolver := func(_ context.Context, pattern string) ([]string, error) {
		if pattern == "foo" {
			return []string{"foo"}, nil
		}
		return []string{}, nil
	}

	for i, tt := range tests {

		t.Run(util.PrettyTestName(tt.path, i), func(t *testing.T) {
			given := AnalyzeUnsupportedCalls(ctx, "GET", tt.path, tt.opaqueId, indexNameResolver)
			assert.Equal(t, tt.isLogged, given)
		})
	}
}

func TestIndexRegexp(t *testing.T) {
	tests := []struct {
		path  string
		index string
	}{
		{"/foo/bar", "foo"},
		{"/foo/_search", "foo"},
		{"/foo/_search/template", "foo"},
		{"/foo/_scripts", "foo"},
		{"/.banana_1.23.4/_doc/some garbage here (Macintosh; Intel Mac OS X 10_15_7) ", ".banana_1.23.4"},
		{"/.reporting-*/_search", ".reporting-*"},
		{"/traces-xx*,xx-*,traces-xx*,x-*,logs-xx*,xx-*/_search", "traces-xx*,xx-*,traces-xx*,x-*,logs-xx*,xx-*"},
	}

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.path, i), func(t *testing.T) {
			given := indexPathRegexp.FindStringSubmatch(tt.path)
			if len(given) > 1 {
				index := given[1]
				assert.Equal(t, tt.index, index)
			} else {
				assert.Fail(t, "No match found")
			}
		})
	}
}
