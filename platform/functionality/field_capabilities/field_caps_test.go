// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package field_capabilities

import (
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFieldCaps(t *testing.T) {
	expected := []byte(`{
   "fields":{
      "arrayOfArraysOfStrings":{
         "keyword":{
            "aggregatable":true,
            "indices":[
               "logs-generic-default"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"keyword"
         }
      },
      "arrayOfArraysOfStrings.text":{
         "text":{
            "aggregatable":false,
            "indices":[
               "logs-generic-default"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"text"
         }
      },
      "arrayOfTuples":{
         "object":{
            "aggregatable":false,
            "indices":[
               "logs-generic-default"
            ],
            "metadata_field":false,
            "searchable":false,
            "type":"object"
         }
      },
      "host.name":{
         "object":{
            "aggregatable":false,
            "indices":[
               "logs-generic-default"
            ],
            "metadata_field":false,
            "searchable":false,
            "type":"object"
         }
      },
      "service.name":{
         "keyword":{
            "aggregatable":true,
            "indices":[
               "logs-generic-default"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"keyword"
         }
      },
      "service.name.text":{
         "text":{
            "aggregatable":false,
            "indices":[
               "logs-generic-default"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"text"
         }
      }
   },
   "indices":[
      "logs-generic-default"
   ]
}
`)
	resp, err := handleFieldCapsIndex(
		map[string]config.IndexConfiguration{
			"logs-generic-default": {
				QueryTarget:  []string{config.ClickhouseTarget},
				IngestTarget: []string{config.ClickhouseTarget},
			},
		}, &schema.StaticRegistry{
			Tables: map[schema.IndexName]schema.Schema{
				"logs-generic-default": {
					Fields: map[schema.FieldName]schema.Field{
						"service.name":           {PropertyName: "service.name", InternalPropertyName: "service.name", Type: schema.QuesmaTypeKeyword},
						"arrayOfArraysOfStrings": {PropertyName: "arrayOfArraysOfStrings", InternalPropertyName: "arrayOfArraysOfStrings", Type: schema.QuesmaTypeKeyword},
						"arrayOfTuples":          {PropertyName: "arrayOfTuples", InternalPropertyName: "arrayOfTuples", Type: schema.QuesmaTypeObject},
						"host.name":              {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					},
				},
			},
		}, []string{"logs-generic-default"})
	assert.NoError(t, err)
	expectedResp, err := json.MarshalIndent(expected, "", "  ")
	assert.NoError(t, err)
	err = json.Unmarshal(expectedResp, &expectedResp)
	assert.NoError(t, err)

	difference1, difference2, err := util.JsonDifference(
		string(resp),
		string(expectedResp),
	)
	assert.NoError(t, err)
	assert.Empty(t, difference1)
	assert.Empty(t, difference2)
}

func TestFieldCapsWithAliases(t *testing.T) {
	expected := []byte(`{
  "fields": {
    "@timestamp": {
      "date": {
        "aggregatable": true,
        "indices": [
          "logs-generic-default"
        ],
        "metadata_field": false,
        "searchable": true,
        "type": "date"
      }
    },
    "timestamp": {
      "date": {
        "aggregatable": true,
        "indices": [
          "logs-generic-default"
        ],
        "metadata_field": false,
        "searchable": true,
        "type": "date"
      }
    }
  },
  "indices": [
    "logs-generic-default"
  ]
}`)
	resp, err := handleFieldCapsIndex(
		map[string]config.IndexConfiguration{"logs-generic-default": {QueryTarget: []string{config.ClickhouseTarget}, IngestTarget: []string{config.ClickhouseTarget}}}, &schema.StaticRegistry{
			Tables: map[schema.IndexName]schema.Schema{
				"logs-generic-default": {
					Fields:  map[schema.FieldName]schema.Field{"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeTimestamp}},
					Aliases: map[schema.FieldName]schema.FieldName{"timestamp": "@timestamp"},
				},
			},
		}, []string{"logs-generic-default"})
	assert.NoError(t, err)
	expectedResp, err := json.MarshalIndent(expected, "", "  ")
	assert.NoError(t, err)
	err = json.Unmarshal(expectedResp, &expectedResp)
	assert.NoError(t, err)

	difference1, difference2, err := util.JsonDifference(
		string(resp),
		string(expectedResp),
	)

	assert.NoError(t, err)
	assert.Empty(t, difference1)
	assert.Empty(t, difference2)
}

func TestFieldCapsMultipleIndexes(t *testing.T) {
	tableMap := database_common.NewTableMap()
	tableMap.Store("logs-1", &database_common.Table{
		Name: "logs-generic-default",
		Cols: map[string]*database_common.Column{
			"foo.bar1": {Name: "foo.bar1", Type: database_common.BaseType{Name: "String"}},
		},
	})
	tableMap.Store("logs-2", &database_common.Table{
		Name: "logs-generic-default",
		Cols: map[string]*database_common.Column{
			"foo.bar2": {Name: "foo.bar2", Type: database_common.BaseType{Name: "String"}},
		},
	})
	resp, err := handleFieldCapsIndex(
		map[string]config.IndexConfiguration{
			"logs-1": {
				QueryTarget:  []string{config.ClickhouseTarget},
				IngestTarget: []string{config.ClickhouseTarget},
			},
			"logs-2": {
				QueryTarget:  []string{config.ClickhouseTarget},
				IngestTarget: []string{config.ClickhouseTarget},
			},
		}, &schema.StaticRegistry{
			Tables: map[schema.IndexName]schema.Schema{
				"logs-1": {
					Fields: map[schema.FieldName]schema.Field{
						"foo.bar1": {PropertyName: "foo.bar1", InternalPropertyName: "foo.bar1", Type: schema.QuesmaTypeText},
					},
				},
				"logs-2": {
					Fields: map[schema.FieldName]schema.Field{
						"foo.bar2": {PropertyName: "foo.bar2", InternalPropertyName: "foo.bar2", Type: schema.QuesmaTypeText},
					},
				},
			},
		}, []string{"logs-1", "logs-2"})
	assert.NoError(t, err)
	expectedResp, err := json.MarshalIndent([]byte(`{
   "fields":{
      "foo.bar1":{
         "text":{
            "aggregatable":false,
            "indices":[
               "logs-1"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"text"
         }
      },
      "foo.bar1.keyword":{
         "keyword":{
            "aggregatable":true,
            "indices":[
               "logs-1"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"keyword"
         }
      },
      "foo.bar2":{
         "text":{
            "aggregatable":false,
            "indices":[
               "logs-2"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"text"
         }
      },
      "foo.bar2.keyword":{
         "keyword":{
            "aggregatable":true,
            "indices":[
               "logs-2"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"keyword"
         }
      }
   },
   "indices":[
      "logs-1",
      "logs-2"
   ]
}
`), "", "  ")
	assert.NoError(t, err)
	err = json.Unmarshal(expectedResp, &expectedResp)
	assert.NoError(t, err)

	difference1, difference2, err := util.JsonDifference(
		string(resp),
		string(expectedResp),
	)
	assert.NoError(t, err)
	assert.Empty(t, difference1)
	assert.Empty(t, difference2)
}

func TestFieldCapsMultipleIndexesConflictingEntries(t *testing.T) {
	tableMap := database_common.NewTableMap()
	tableMap.Store("logs-1", &database_common.Table{
		Name: "logs-1",
		Cols: map[string]*database_common.Column{
			"foo.bar": {Name: "foo.bar", Type: database_common.BaseType{Name: "String"}},
		},
	})
	tableMap.Store("logs-2", &database_common.Table{
		Name: "logs-2",
		Cols: map[string]*database_common.Column{
			"foo.bar": {Name: "foo.bar", Type: database_common.BaseType{Name: "Boolean"}},
		},
	})
	tableMap.Store("logs-3", &database_common.Table{
		Name: "logs-3",
		Cols: map[string]*database_common.Column{
			"foo.bar": {Name: "foo.bar", Type: database_common.BaseType{Name: "Boolean"}},
		},
	})
	resp, err := handleFieldCapsIndex(
		map[string]config.IndexConfiguration{
			"logs-1": {
				QueryTarget:  []string{config.ClickhouseTarget},
				IngestTarget: []string{config.ClickhouseTarget},
			},
			"logs-2": {
				QueryTarget:  []string{config.ClickhouseTarget},
				IngestTarget: []string{config.ClickhouseTarget},
			},
			"logs-3": {
				QueryTarget:  []string{config.ClickhouseTarget},
				IngestTarget: []string{config.ClickhouseTarget},
			},
		}, &schema.StaticRegistry{
			Tables: map[schema.IndexName]schema.Schema{
				"logs-1": {
					Fields: map[schema.FieldName]schema.Field{
						"foo.bar": {PropertyName: "foo.bar", InternalPropertyName: "foo.bar", Type: schema.QuesmaTypeKeyword},
					},
				},
				"logs-2": {
					Fields: map[schema.FieldName]schema.Field{
						"foo.bar": {PropertyName: "foo.bar", InternalPropertyName: "foo.bar", Type: schema.QuesmaTypeBoolean},
					},
				},
				"logs-3": {
					Fields: map[schema.FieldName]schema.Field{
						"foo.bar": {PropertyName: "foo.bar", InternalPropertyName: "foo.bar", Type: schema.QuesmaTypeBoolean},
					},
				},
			},
		}, []string{"logs-1", "logs-2", "logs-3"})
	assert.NoError(t, err)
	expectedResp, err := json.MarshalIndent([]byte(`{
   "fields":{
      "foo.bar":{
         "boolean":{
            "aggregatable":true,
            "indices":[
               "logs-2",
               "logs-3"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"boolean"
         },
         "keyword":{
            "aggregatable":true,
            "indices":[
               "logs-1"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"keyword"
         }
      },
      "foo.bar.text":{
         "text":{
            "aggregatable":false,
            "indices":[
               "logs-1"
            ],
            "metadata_field":false,
            "searchable":true,
            "type":"text"
         }
      }
   },
   "indices":[
      "logs-1",
      "logs-2",
      "logs-3"
   ]
}
`), "", "  ")
	assert.NoError(t, err)
	err = json.Unmarshal(expectedResp, &expectedResp)
	assert.NoError(t, err)
	difference1, difference2, err := util.JsonDifference(
		string(resp),
		string(expectedResp),
	)

	assert.NoError(t, err)
	assert.Empty(t, difference1)
	assert.Empty(t, difference2)
}

func Test_merge(t *testing.T) {
	type args struct {
		cap1 model.FieldCapability
		cap2 model.FieldCapability
	}
	tests := []struct {
		name   string
		args   args
		want   model.FieldCapability
		merged bool
	}{
		{
			name: "different types",
			args: args{
				cap1: model.FieldCapability{Type: "text"},
				cap2: model.FieldCapability{Type: "keyword"},
			},
			want:   model.FieldCapability{},
			merged: false,
		},
		{
			name: "same types, different indices",
			args: args{
				cap1: model.FieldCapability{Type: "keyword", Aggregatable: true, MetadataField: util.Pointer(false), Indices: []string{"b", "a"}},
				cap2: model.FieldCapability{Type: "keyword", Aggregatable: true, MetadataField: util.Pointer(false), Indices: []string{"b"}},
			},
			want:   model.FieldCapability{Type: "keyword", Aggregatable: true, MetadataField: util.Pointer(false), Indices: []string{"a", "b"}},
			merged: true,
		},
	}
	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			got, got1 := tt.args.cap1.Concat(tt.args.cap2)
			assert.Equalf(t, tt.want, got, "merge(%v, %v)", tt.args.cap1, tt.args.cap2)
			assert.Equalf(t, tt.merged, got1, "merge(%v, %v)", tt.args.cap1, tt.args.cap2)
		})
	}
}
