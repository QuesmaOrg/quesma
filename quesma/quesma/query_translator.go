package quesma

import (
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/queryparser"
)

type ClickhouseQueryTranslator struct {
	clickhouseLM *clickhouse.LogManager
}

type ClickhouseResultReader struct {
	clickhouseLM *clickhouse.LogManager
}

// TODO come back to (int, error) return type?
func (cw *ClickhouseQueryTranslator) Write(buf []byte) queryparser.Query {
	//fmt.Println("ClickhouseQueryTranslator.Write, buf: ", string(buf))
	query := cw.parseQuery(string(buf))
	//fmt.Printf("ClickhouseQueryTranslator.Write, query: %+v", query)
	return query
}

func (cw *ClickhouseQueryTranslator) WriteAsyncSearch(buf []byte) (queryparser.Query, QueryInfo) {
	fmt.Println("ClickhouseQueryTranslator.WriteAsyncSearch, buf: ", string(buf))
	query, queryInfo := cw.parseQueryAsyncSearch(string(buf))
	fmt.Printf("ClickhouseQueryTranslator.WriteAsyncSearch, queryInfo: %+v, query: %+v", queryInfo, query)
	return query, queryInfo
}

func (cw *ClickhouseResultReader) Read(buf []byte) (int, error) {
	return 0, nil
}
