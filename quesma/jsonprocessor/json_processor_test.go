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
		name   string
		ingres string
		want   string
	}{
		{
			name:   "Rewrite array of objects",
			ingres: `{}`,
			want:   `{}`,
		},
		{
			name:   "Rewrite array of objects",
			ingres: `{"key": 1, "array": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]}`,
			want:   `{"key": 1, "array": {"a": [1, 3], "b": [2, 4]}}`,
		},
	}

	toJson := func(data map[string]interface{}) string {
		jsonData, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		return string(jsonData)
	}

	toMap := func(jsonStr string) map[string]interface{} {
		data, err := types.ParseJSON(jsonStr)
		if err != nil {
			t.Fatal(err)
		}
		return data

	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := &RewriteArrayOfObject{}

			ingres := toMap(tt.ingres)

			got, err := processor.Transform(ingres)
			if err != nil {
				t.Fatal(err)
			}

			wantJson := toJson(toMap(tt.want)) // reformat the expected JSON
			gotJson := toJson(got)

			assert.Equal(t, wantJson, gotJson)

		})
	}

}
