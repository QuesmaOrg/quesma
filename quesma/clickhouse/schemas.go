package clickhouse

var NewRuntimeSchemas = make(TableMap)

// A solution for now to remember tables. Didn't want to bother with config files at POC stage.
// Generated via DumpTableSchemas() and later ShortenDumpSchemasOutput()
var PredefinedTableSchemas = TableMap{
	"/device_logs/_doc": &Table{
		Created:  false,
		Name:     "/device_logs/_doc",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"event_name": {
				Name: "event_name",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Modifiers: "NOT NULL CODEC(ZSTD(1))",
			},
		},
		indexes: []IndexStatement{
			getIndexStatement("event_name"),
		},
		Config: &ChTableConfig{
			hasTimestamp:         true,
			timestampDefaultsNow: false,
			engine:               "MergeTree",
			orderBy:              "(timestamp)",
			partitionBy:          "",
			primaryKey:           "",
			ttl:                  "",
			settings:             "index_granularity = 8192, ttl_only_drop_parts = 1",
			hasOthers:            false,
			attributes: []Attribute{
				NewDefaultStringAttribute(),
			},
			castUnsupportedAttrValueTypesToString: true,
			preferCastingToOthers:                 true,
		},
	},
}
