package quesma

import (
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/queryparser"
	"time"
)

// Feel free to suggest a better name for this file

func (cw *ClickhouseQueryTranslator) getAttributesList(tableName string) []clickhouse.Attribute {
	return cw.clickhouseLM.GetAttributesList(tableName)
}

func (cw *ClickhouseQueryTranslator) getFieldInfo(tableName string, fieldName string) clickhouse.FieldInfo {
	return cw.clickhouseLM.GetFieldInfo(tableName, fieldName)
}

// TODO flatten tuples, I think (or just don't support them for now, we don't want them at the moment in production schemas)
func (cw *ClickhouseQueryTranslator) getFieldsList(tableName string) []string {
	return []string{"message"}
}

func (cw *ClickhouseQueryTranslator) queryClickhouse(query queryparser.Query) ([]clickhouse.QueryResultRow, error) {
	return cw.clickhouseLM.ProcessSelectQuery(query)
}

// fieldName = "*" -> we query all, otherwise only this 1 field
func (cw *ClickhouseQueryTranslator) getNMostRecentRows(tableName, fieldName, timestampFieldName, originalSelectStmt string, limit int) ([]clickhouse.QueryResultRow, error) {
	return cw.clickhouseLM.GetNMostRecentRows(tableName, fieldName, timestampFieldName, originalSelectStmt, limit)
}

func (cw *ClickhouseQueryTranslator) getHistogram(tableName string) ([]clickhouse.HistogramResult, error) {
	return cw.clickhouseLM.GetHistogram(tableName, "@timestamp", 15*time.Minute)
}

//lint:ignore U1000 Not used yet
func (cw *ClickhouseQueryTranslator) getAutocompleteSuggestions(tableName, fieldName string, prefix string, limit int) ([]clickhouse.QueryResultRow, error) {
	return cw.clickhouseLM.GetAutocompleteSuggestions(tableName, fieldName, prefix, limit)
}

func (cw *ClickhouseQueryTranslator) getFacets(tableName, fieldName, originalSelectStmt string, limit int) ([]clickhouse.QueryResultRow, error) {
	return cw.clickhouseLM.GetFacets(tableName, fieldName, originalSelectStmt, limit)
}
