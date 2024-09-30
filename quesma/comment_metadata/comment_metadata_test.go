// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package comment_metadata

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCommentMetadata_Marshall(t *testing.T) {

	tests := []struct {
		name  string
		input map[string]string
		want  string
	}{
		{
			name: "test1",
			input: map[string]string{
				"foo": "bar",
			},
			want: "quesmaMetadataV1:foo=bar",
		},
		{
			name: "test2",
			input: map[string]string{
				"łąś": "żółć",
			},
			want: "quesmaMetadataV1:%C5%82%C4%85%C5%9B=%C5%BC%C3%B3%C5%82%C4%87",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := NewCommentMetadata()
			cm.Values = tt.input

			if got := cm.Marshall(); got != tt.want {
				t.Errorf("Marshall() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnmarshallCommentMetadata(t *testing.T) {

	tests := []struct {
		name  string
		input string
		want  map[string]string
		fail  bool
	}{
		{
			name:  "simple",
			input: "quesmaMetadataV1:foo=bar",
			want: map[string]string{
				"foo": "bar",
			},
			fail: false,
		},
		{
			name:  "with special characters",
			input: "quesmaMetadataV1:%C5%82%C4%85%C5%9B=%C5%BC%C3%B3%C5%82%C4%87",
			want: map[string]string{
				"łąś": "żółć",
			},
			fail: false,
		},
		{
			name:  "with human comments ",
			input: "some comment here  quesmaMetadataV1:foo=bar  and here ",
			want: map[string]string{
				"foo": "bar",
			},
			fail: false,
		},
		{
			name:  "with human comments invalid version ",
			input: "some comment here  quesmaMetadataV2:foo=bar  and here ",
			want: map[string]string{
				"foo": "bar",
			},
			fail: true,
		},
		{
			name:  "no metadata ",
			input: "some comment here    and here ",
			want:  nil,
			fail:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm, err := UnmarshallCommentMetadata(tt.input)

			if tt.fail {
				if err == nil {
					t.Fatal("Expecting error, got nil")
				}
				return
			} else {
				if err != nil {
					t.Fatal("Unexpected error ", err)
				}
			}

			if tt.want == nil && cm != nil {
				t.Fatal("Expecting nil, got ", cm)
			}

			if tt.want == nil && cm == nil {
				return
			}

			assert.Equal(t, tt.want, cm.Values)
		})
	}
}
