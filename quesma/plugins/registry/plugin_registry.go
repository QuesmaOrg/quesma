package registry

import (
	"mitmproxy/quesma/plugins"
	"mitmproxy/quesma/plugins/elastic_clickhouse_fields"
)

// TODO plugins registry
// TODO plugins finder

// legacy, tests are passing with this
var DefaultPlugin plugins.Plugin = &elastic_clickhouse_fields.LegacyClickhouseDoubleColonsPlugin{}

// we return fields in the format of elastic
// it may require fixing the tests, but kibana works with this
//var DefaultPlugin plugins.Plugin = &elastic_clickhouse_fields.ClickhouseDoubleColonsElasticDotsPlugin{}

// ultimate future
//var DefaultPlugin plugins.Plugin = &elastic_clickhouse_fields.ClickhouseSQLNativeLElasticDotsPlugin{}
