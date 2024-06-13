package registry

import (
	"mitmproxy/quesma/plugins"
	"mitmproxy/quesma/plugins/elastic_clickhouse_fields"
)

// TODO plugins registry
// TODO plugins finder

// now
var DefaultPlugin plugins.Plugin = &elastic_clickhouse_fields.LegacyClickhouseDoubleColonsPlugin{}

// near future
// var DefaultPlugin plugins.Plugin = &elastic_clickhouse_fields.ClickhouseDoubleColonsElasticDotsPlugin{}

// ultimate future
//var DefaultPlugin plugins.Plugin = &elastic_clickhouse_fields.ClickhouseSQLNativeLElasticDotsPlugin{}
