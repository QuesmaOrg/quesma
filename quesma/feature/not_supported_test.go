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
	cfg.IndexConfig = []config.IndexConfiguration{
		{
			NamePattern: "foo",
			Enabled:     true,
		},
	}

	ctx := context.Background()

	indexNameResolver := func(pattern string) []string {
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
