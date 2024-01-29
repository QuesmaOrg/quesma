package jsonprocessor

import (
	"reflect"
	"testing"
)

func TestFlattenMap(t *testing.T) {
	tests := []struct {
		name string
		data map[string]interface{}
		want map[string]interface{}
	}{
		{
			name: "Flatten simple map",
			data: map[string]interface{}{"key1": "value1", "key2": "value2"},
			want: map[string]interface{}{"key1": "value1", "key2": "value2"},
		},
		{
			name: "Flatten nested map",
			data: map[string]interface{}{
				"key1": "value1",
				"key2": map[string]interface{}{
					"nestedKey1": "nestedValue1",
					"nestedKey2": "nestedValue2",
				},
			},
			want: map[string]interface{}{
				"key1":             "value1",
				"key2::nestedKey1": "nestedValue1",
				"key2::nestedKey2": "nestedValue2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FlattenMap(tt.data, "::"); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FlattenMap() = %v, want %v", got, tt.want)
			}
		})
	}
}
