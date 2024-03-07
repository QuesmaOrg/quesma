package clickhouse

import (
	"mitmproxy/quesma/concurrent"
	"reflect"
)

var predefined = map[string]*Table{
	"device_logs": {
		Created:      false,
		Name:         "device_logs",
		DatabaseName: "",
		Cluster:      "",
		Cols: map[string]*Column{
			"event_name":                              lowCardinalityString("event_name"),
			"event_section":                           lowCardinalityString("event_section"),
			"dedup_id":                                genericString("dedup_id"),
			"user_id":                                 genericString("user_id"),
			"client_id":                               genericString("client_id"),
			"client_ip":                               genericString("client_ip"),
			"ftd_session_time":                        int64CH("ftd_session_time"),
			"timestamps::topology_entry_time":         genericString("timestamps::topology_entry_time"),
			"ts_day":                                  lowCardinalityString("ts_day"),
			"et_day":                                  genericString("et_day"),
			"et_day_hour":                             genericString("et_day_hour"),
			"ts_day_hour":                             genericString("ts_day_hour"),
			"ts_time_druid":                           dateTime("ts_time_druid"), // TODO TZ
			"epoch_time_original":                     int64CH("epoch_time_original"),
			"epoch_time":                              dateTime("epoch_time"), // TODO TZ
			"properties::isreg":                       boolean("properties::isreg"),
			"properties::enriched_client_ip":          genericString("properties::enriched_client_ip"),
			"properties::enriched_app_id":             genericString("properties::enriched_app_id"),
			"properties::enriched_user_id":            genericString("properties::enriched_user_id"),
			"properties::app_id":                      genericString("properties::app_id"),
			"properties::server_loc":                  genericString("properties::server_loc"),
			"properties::pv_event":                    lowCardinalityString("properties::pv_event"),
			"properties::ab_NewsStickyType":           lowCardinalityString("properties::ab_NewsStickyType"),
			"properties::signed_state":                lowCardinalityString("properties::signed_state"),
			"properties::country_detection_mechanism": lowCardinalityString("properties::country_detection_mechanism"),
			"properties::enriched_user_language_primary": lowCardinalityString("properties::enriched_user_language_primary"),
			"properties::selected_country":               lowCardinalityString("properties::selected_country"),
			"properties::user_os_name":                   lowCardinalityString("properties::user_os_name"),
			"properties::user_os_ver":                    lowCardinalityString("properties::user_os_ver"),
			"properties::referrer_action":                lowCardinalityString("properties::referrer_action"),
			"properties::user_type":                      lowCardinalityString("properties::user_type"),
			"properties::user_language_primary":          lowCardinalityString("properties::user_language_primary"),
			"properties::user_handset_model":             lowCardinalityString("properties::user_handset_model"),
			"properties::user_handset_maker":             lowCardinalityString("properties::user_handset_maker"),
			"properties::user_feed_type":                 lowCardinalityString("properties::user_feed_type"),
			"properties::user_app_ver":                   lowCardinalityString("properties::user_app_ver"),
			"properties::tabtype":                        lowCardinalityString("properties::tabtype"),
			"properties::network_service_provider":       lowCardinalityString("properties::network_service_provider"),
		},
		indexes: []IndexStatement{
			getIndexStatement("event_name"),
		},
		Config: &ChTableConfig{
			hasTimestamp:         false,
			timestampDefaultsNow: false,
			engine:               "MergeTree",
			orderBy:              "epoch_time",
			partitionBy:          "toYYYYMM(epoch_time)",
			primaryKey:           "",
			ttl:                  "toDateTime(epoch_time) + INTERVAL 20 MINUTE",
			settings:             "index_granularity = 8192",
			hasOthers:            false,
			attributes: []Attribute{
				NewDefaultStringAttribute(),
			},
			castUnsupportedAttrValueTypesToString: true,
			preferCastingToOthers:                 true,
		},
	},
}

func withPredefinedTables() TableMap {
	var m = concurrent.NewMap[string, *Table]()
	for k, v := range predefined {
		m.Store(k, v)
	}
	return *m
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

func boolean(name string) *Column {
	return &Column{
		Name:      name,
		Type:      BaseType{Name: "Bool", goType: reflect.TypeOf(true)},
		Modifiers: "",
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
			goType: NewBaseType("DateTime64").goType,
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
