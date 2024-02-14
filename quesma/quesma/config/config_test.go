package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIndexConfiguration_Matches(t *testing.T) {
	type fields struct {
		NamePattern string
		Enabled     bool
	}
	tests := []struct {
		name      string
		fields    fields
		indexName string
		want      bool
	}{
		{"logs-generic-default", fields{"logs-generic-default", true}, "logs-generic-default", true},
		{"logs-generic-default", fields{"logs-generic-default", true}, "logs-generic-default2", false},
		{"logs-generic-*", fields{"logs-generic-*", true}, "logs-generic-default", true},
		{"logs-generic-*", fields{"logs-generic-*", true}, "logs2-generic-default", false},
		{"logs-*-*", fields{"logs-*-*", true}, "logs-generic-default", true},
		{"logs-*-*", fields{"logs-*-*", true}, "generic-default", false},
		{"logs-*", fields{"logs-*", true}, "logs-generic-default", true},
		{"logs-*", fields{"logs-*", true}, "blogs-generic-default", false},
		{"*", fields{"*", true}, "logs-generic-default", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := IndexConfiguration{
				NamePattern: tt.fields.NamePattern,
				Enabled:     tt.fields.Enabled,
			}
			assert.Equalf(t, tt.want, c.Matches(tt.indexName), "Matches(%v)", tt.indexName)
		})
	}
}
