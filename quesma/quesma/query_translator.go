package quesma

import (
	"fmt"
	"mitmproxy/quesma/clickhouse"
)

type ClickhouseQueryTranslator struct {
	clickhouseLM *clickhouse.LogManager
}

type ClickhouseResultReader struct {
	clickhouseLM *clickhouse.LogManager
}

// TODO come back to (int, error) return type?
func (cw *ClickhouseQueryTranslator) Write(buf []byte) Query {
	fmt.Println("ClickhouseQueryTranslator.Write, buf: ", string(buf))
	query := cw.parseQuery(string(buf))
	fmt.Printf("ClickhouseQueryTranslator.Write, query: %+v", query)
	return query
}

func (cw *ClickhouseResultReader) Read(buf []byte) (int, error) {
	return 0, nil
}
