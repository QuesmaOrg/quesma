package clickhouse

var NewRuntimeSchemas = make(TableMap)

// A solution for now to remember tables. Didn't want to bother with config files at POC stage.
// Generated via DumpTableSchemas() and later ShortenDumpSchemasOutput()
var PredefinedTableSchemas = TableMap{
	"device_logs": &Table{
		Created:  false,
		Name:     "device_logs",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"event_name":          lowCardinalityString("event_name"),
			"event_section":       lowCardinalityString("event_section"),
			"dedup_id":            genericString("dedup_id"),
			"user_id":             genericString("user_id"),
			"client_id":           genericString("client_id"),
			"client_ip":           genericString("client_ip"),
			"ts_day":              lowCardinalityString("ts_day"),
			"et_day_hour":         genericString("et_day_hour"),
			"ts_day_hour":         genericString("ts_day_hour"),
			"ts_time_druid":       dateTime("ts_time_druid"), // TODO TZ
			"epoch_time_original": int64CH("epoch_time_original"),
			"epoch_time":          dateTime("epoch_time"), // TODO TZ
		},
		indexes: []IndexStatement{
			getIndexStatement("event_name"),
		},
		Config: &ChTableConfig{
			hasTimestamp:         false,
			timestampDefaultsNow: false,
			engine:               "MergeTree",
			orderBy:              "(epoch_time_original)",
			partitionBy:          "toDate(epoch_time_original / 1000000000)",
			primaryKey:           "",
			ttl:                  "toDateTime(epoch_time_original / 1000000000) + toIntervalSecond(1296000)",
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

func genericString(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "String",
			goType: NewBaseType("String").goType,
		},
		Modifiers: "CODEC(ZSTD(1))",
	}
}

func lowCardinalityString(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "LowCardinality(String)",
			goType: NewBaseType("LowCardinality(String)").goType,
		},
	}
}

func dateTime(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "DateTime64",
			goType: nil,
		},
		Modifiers: "CODEC(DoubleDelta, LZ4)",
	}
}

func int64CH(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "Int64",
			goType: NewBaseType("Int64").goType,
		},
		Modifiers: "CODEC(DoubleDelta, LZ4)",
	}
}
