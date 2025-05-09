// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"github.com/QuesmaOrg/quesma/platform/util"
	"reflect"
	"testing"
)

func TestFieldCapability_Concat(t *testing.T) {
	tests := []struct {
		name   string
		fc1    FieldCapability
		fc2    FieldCapability
		result FieldCapability
		merged bool
	}{
		{
			name:   "Two text FieldCapabilities, different indices",
			fc1:    FieldCapability{Type: "text", Indices: []string{"a"}, MetadataField: util.Pointer(false)},
			fc2:    FieldCapability{Type: "text", Indices: []string{"b"}, MetadataField: util.Pointer(false)},
			result: FieldCapability{Type: "text", Indices: []string{"a", "b"}, MetadataField: util.Pointer(false)},
			merged: true,
		},
		{
			name:   "Two text FieldCapabilities, nil MetadataField",
			fc1:    FieldCapability{Type: "text", Indices: []string{"a"}, MetadataField: nil},
			fc2:    FieldCapability{Type: "text", Indices: []string{"b"}, MetadataField: nil},
			result: FieldCapability{Type: "text", Indices: []string{"a", "b"}, MetadataField: nil},
			merged: true,
		},
		{
			name:   "Two text FieldCapabilities, different indices, one non-aggregatable and non-searchable",
			fc1:    FieldCapability{Type: "text", Searchable: false, Aggregatable: true, Indices: []string{"a"}, MetadataField: util.Pointer(false)},
			fc2:    FieldCapability{Type: "text", Searchable: true, Aggregatable: false, Indices: []string{"b"}, MetadataField: util.Pointer(false)},
			result: FieldCapability{Type: "text", Searchable: false, Aggregatable: false, Indices: []string{"a", "b"}, MetadataField: util.Pointer(false)},
			merged: true,
		},
		{
			name:   "Two text FieldCapabilities, same index",
			fc1:    FieldCapability{Type: "text", Indices: []string{"a"}, MetadataField: util.Pointer(false)},
			fc2:    FieldCapability{Type: "text", Indices: []string{"a"}, MetadataField: util.Pointer(false)},
			result: FieldCapability{Type: "text", Indices: []string{"a"}, MetadataField: util.Pointer(false)},
			merged: true,
		},
		{
			name:   "Two incompatible FieldCapabilities",
			fc1:    FieldCapability{Type: "text", Indices: []string{"a"}, MetadataField: util.Pointer(false)},
			fc2:    FieldCapability{Type: "ip", Indices: []string{"a"}, MetadataField: util.Pointer(false)},
			result: FieldCapability{},
			merged: false,
		},
	}
	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			got, got1 := tt.fc1.Concat(tt.fc2)
			if got1 != tt.merged {
				t.Errorf("Concat() got1 = %v, want %v", got1, tt.merged)
			}
			if !reflect.DeepEqual(got, tt.result) {
				t.Errorf("Concat() got = %+v, want %+v", got, tt.result)
			}
		})
	}
}
