package jsonprocessor

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/quesma/types"
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

func TestRewriteArrayOfObject_Transform(t *testing.T) {

	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "Rewrite array of objects",
			data: `{}`,
			want: `{}`,
		},
		{
			name: "Rewrite array of objects",
			data: `{"key": 1, "array": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]}`,
			want: `{"key": 1, "array": {"a": [1, 3], "b": [2, 4]}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := &RewriteArrayOfObject{}

			data, err := types.ParseJSON(tt.data)
			if err != nil {
				t.Fatal(err)
			}

			want, err := types.ParseJSON(tt.want)
			if err != nil {
				t.Fatal(err)
			}

			got, err := processor.Transform(data)
			if err != nil {
				t.Fatal(err)
			}

			wantJson, err := json.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}

			gotJson, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}

			wantJsonStr := string(wantJson)
			gotJsonStr := string(gotJson)

			assert.Equal(t, wantJsonStr, gotJsonStr)

		})
	}

}
