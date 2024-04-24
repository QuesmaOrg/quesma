package feature

import (
	"context"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/quesma/config"
	"testing"
)

func TestNewUnsupportedFeature_index(t *testing.T) {

	tests := []struct {
		path     string
		isLogged bool
	}{
		{"/foo/_search", true},
		{"/foo/_new_feature", true},
		{"/bar/_search", false},

		{"/foo/_search/template", true},
		{"/_scripts/foo", true},
	}

	cfg := config.QuesmaConfiguration{}
	cfg.IndexConfig = map[string]config.IndexConfiguration{
		"foo": {
			Name:    "foo",
			Enabled: true,
		},
	}

	ctx := context.Background()

	indexNameResolver := func(_ context.Context, pattern string) []string {
		if pattern == "foo" {
			return []string{"foo"}
		}
		return []string{}
	}

	for _, tt := range tests {

		t.Run(tt.path, func(t *testing.T) {
			given := AnalyzeUnsupportedCalls(ctx, "GET", tt.path, indexNameResolver)
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

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
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
