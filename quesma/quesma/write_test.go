package quesma

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_matches(t *testing.T) {
	type args struct {
		indexName        string
		indexNamePattern string
	}
	tests := []struct {
		args args
		want bool
	}{
		{args: args{"logs-generic-default", "logs-generic-default*"}, want: true},
		{args: args{"logs-generic-default", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-default-foo", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-", "logs-generic-*"}, want: true},
		{args: args{"logs-generic", "logs-generic-*"}, want: false},
		{args: args{"logs2-generic", "logs-generic-*"}, want: false},
		{args: args{"logs-generic-default", "logs-*-default"}, want: true},
		{args: args{"logs-specific", "logs-generic-*"}, want: false},
		{args: args{"logs-generic-123", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-default-foo-bar", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-abc", "logs-generic-*"}, want: true},
		{args: args{"logs-custom-default", "logs-*-default"}, want: true},
		{args: args{"logs-custom-default", "logs-generic-*"}, want: false},
		{args: args{"logs-custom-specific", "logs-custom-*"}, want: true},
		{args: args{"logs-custom-specific-123", "logs-custom-*"}, want: true},
		{args: args{"logs-custom-abc", "logs-custom-*"}, want: true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s->%s[%v]", tt.args.indexName, tt.args.indexNamePattern, tt.want), func(t *testing.T) {
			assert.Equalf(t, tt.want, matches(tt.args.indexName, tt.args.indexNamePattern), "matches(%v, %v)", tt.args.indexName, tt.args.indexNamePattern)
		})
	}
}
