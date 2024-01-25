package queryparser

import (
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"time"
)

type ClickhouseQueryTranslator struct {
	ClickhouseLM *clickhouse.LogManager
}

type ClickhouseResultReader struct {
	ClickhouseLM *clickhouse.LogManager
}

// TODO come back to (int, error) return type?
func (cw *ClickhouseQueryTranslator) Write(buf []byte) model.Query {
	//fmt.Println("ClickhouseQueryTranslator.Write, buf: ", string(buf))
	query := cw.parseQuery(string(buf))
	//fmt.Printf("ClickhouseQueryTranslator.Write, query: %+v", query)
	return query
}

func (cw *ClickhouseQueryTranslator) WriteAsyncSearch(buf []byte) (model.Query, model.QueryInfo) {
	fmt.Println("ClickhouseQueryTranslator.WriteAsyncSearch, buf: ", string(buf))
	query, queryInfo := cw.parseQueryAsyncSearch(string(buf))
	fmt.Printf("ClickhouseQueryTranslator.WriteAsyncSearch, queryInfo: %+v, query: %+v", queryInfo, query)
	return query, queryInfo
}

func (cw *ClickhouseResultReader) Read(buf []byte) (int, error) {
	return 0, nil
}

func (cw *ClickhouseQueryTranslator) GetAttributesList(tableName string) []clickhouse.Attribute {
	return cw.ClickhouseLM.GetAttributesList(tableName)
}

func (cw *ClickhouseQueryTranslator) GetFieldInfo(tableName string, fieldName string) clickhouse.FieldInfo {
	return cw.ClickhouseLM.GetFieldInfo(tableName, fieldName)
}

// TODO flatten tuples, I think (or just don't support them for now, we don't want them at the moment in production schemas)
func (cw *ClickhouseQueryTranslator) GetFieldsList(tableName string) []string {
	return []string{"message"}
}

func (cw *ClickhouseQueryTranslator) QueryClickhouse(query model.Query) ([]clickhouse.QueryResultRow, error) {
	return cw.ClickhouseLM.ProcessSelectQuery(query)
}

// fieldName = "*" -> we query all, otherwise only this 1 field
func (cw *ClickhouseQueryTranslator) GetNMostRecentRows(tableName, fieldName, timestampFieldName, originalSelectStmt string, limit int) ([]clickhouse.QueryResultRow, error) {
	return cw.ClickhouseLM.GetNMostRecentRows(tableName, fieldName, timestampFieldName, originalSelectStmt, limit)
}

func (cw *ClickhouseQueryTranslator) GetHistogram(tableName string) ([]clickhouse.HistogramResult, error) {
	return cw.ClickhouseLM.GetHistogram(tableName, "@timestamp", 15*time.Minute)
}

//lint:ignore U1000 Not used yet
func (cw *ClickhouseQueryTranslator) GetAutocompleteSuggestions(tableName, fieldName string, prefix string, limit int) ([]clickhouse.QueryResultRow, error) {
	return cw.ClickhouseLM.GetAutocompleteSuggestions(tableName, fieldName, prefix, limit)
}

func (cw *ClickhouseQueryTranslator) GetFacets(tableName, fieldName, originalSelectStmt string, limit int) ([]clickhouse.QueryResultRow, error) {
	return cw.ClickhouseLM.GetFacets(tableName, fieldName, originalSelectStmt, limit)
}
