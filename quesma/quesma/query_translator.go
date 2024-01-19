package quesma

import (
	"mitmproxy/quesma/clickhouse"
)

type ClickhouseQueryTranslator struct {
	clickhouseLM *clickhouse.LogManager
}

type ClickhouseResultReader struct {
	clickhouseLM *clickhouse.LogManager
}

func (cw *ClickhouseQueryTranslator) Write(buf []byte) (int, error) {
	return 0, nil
}

func (cw *ClickhouseResultReader) Read(buf []byte) (int, error) {
	return 0, nil
}
