package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_parseOperationMode(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want OperationMode
	}{
		{name: "proxy", args: args{"proxy"}, want: Proxy},
		{name: "proxy-inspect", args: args{"proxy-inspect"}, want: ProxyInspect},
		{name: "dual-write-query-elastic", args: args{"dual-write-query-elastic"}, want: DualWriteQueryElastic},
		{name: "dual-write-query-clickhouse", args: args{"dual-write-query-clickhouse"}, want: DualWriteQueryClickhouse},
		{name: "dual-write-query-clickhouse-verify", args: args{"dual-write-query-clickhouse-verify"}, want: DualWriteQueryClickhouseVerify},
		{name: "dual-write-query-clickhouse-fallback", args: args{"dual-write-query-clickhouse-fallback"}, want: DualWriteQueryClickhouseFallback},
		{name: "clickhouse", args: args{"clickhouse"}, want: ClickHouse},
		{name: "unknown", args: args{"unknown"}, want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, parseOperationMode(tt.args.str), "parseOperationMode(%v)", tt.args.str)
		})
	}
}
