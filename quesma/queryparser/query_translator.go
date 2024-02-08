package queryparser

import (
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"strconv"
	"time"
)

type JsonMap = map[string]interface{}

type ClickhouseQueryTranslator struct {
	ClickhouseLM *clickhouse.LogManager
}

func makeResponseSearchQueryNormal[T fmt.Stringer](ResultSet []T) ([]byte, error) {
	hits := make([]model.SearchHit, len(ResultSet))
	for i, row := range ResultSet {
		hits[i] = model.SearchHit{
			Source: []byte(row.String()),
		}
	}
	response := model.SearchResp{
		Hits: model.SearchHits{
			Hits: hits,
			Total: model.Total{
				Value:    len(ResultSet),
				Relation: "eq",
			},
		},
	}
	return json.MarshalIndent(response, "", "  ")
}

func makeResponseSearchQueryCount[T fmt.Stringer](ResultSet []T) ([]byte, error) {
	aggregations := model.Aggregations{
		"suggestions": {
			"doc_count_error_upper_bound": 0,
			"sum_other_doc_count":         0,
			"buckets":                     []interface{}{},
		},
		"unique_terms": {
			"value": 0,
		},
	}
	response := model.SearchResp{
		Aggregations:      aggregations,
		DidTerminateEarly: new(bool), // a bit hacky with pointer, but seems like the only way https://stackoverflow.com/questions/37756236/json-golang-boolean-omitempty
		Hits: model.SearchHits{
			Hits: []model.SearchHit{},
			Total: model.Total{
				Value:    len(ResultSet),
				Relation: "eq",
			},
		},
	}
	return json.MarshalIndent(response, "", "  ")
}

func MakeResponseSearchQuery[T fmt.Stringer](ResultSet []T, typ model.SearchQueryType) ([]byte, error) {
	switch typ {
	case model.Normal:
		return makeResponseSearchQueryNormal(ResultSet)
	case model.Count:
		return makeResponseSearchQueryCount(ResultSet)
	}
	return nil, fmt.Errorf("unknown SearchQueryType: %v", typ)
}

func MakeResponseAsyncSearchAggregated(ResultSet []clickhouse.QueryResultRow, typ model.AsyncSearchQueryType) ([]byte, error) {
	buckets := make([]JsonMap, len(ResultSet))
	for i, row := range ResultSet {
		buckets[i] = make(JsonMap)
		for _, col := range row.Cols {
			buckets[i][col.ColName] = col.Value
		}
	}
	var sampleCount uint64 // uint64 because that's what clickhouse reader returns
	for _, row := range ResultSet {
		sampleCount += row.Cols[clickhouse.DocCount].Value.(uint64)
	}

	var id *string
	aggregations := model.Aggregations{}
	switch typ {
	case model.Histogram:
		aggregations["0"] = JsonMap{
			"buckets": buckets,
		}
		id = new(string)
		*id = "fake-id"
	case model.AggsByField:
		aggregations["sample"] = JsonMap{
			"doc_count": int(sampleCount),
			"sample_count": JsonMap{
				"value": int(sampleCount),
			},
			"top_values": JsonMap{
				"buckets":                     buckets,
				"sum_other_doc_count":         0,
				"doc_count_error_upper_bound": 0,
			},
		}
	default:
		return nil, fmt.Errorf("unknown AsyncSearchAggregatedQueryType: %v", typ)
	}

	response := model.AsyncSearchEntireResp{
		Response: model.AsyncSearchResp{
			Aggregations: aggregations,
			Hits: model.AsyncSearchHits{
				Hits: []model.AsyncSearchHit{}, // seems redundant, but can't remove this, created JSON won't match
				Total: &model.Total{
					Value:    int(sampleCount),
					Relation: "eq",
				},
			},
		},
		ID: id,
	}
	return json.MarshalIndent(response, "", "  ")
}

func MakeResponseAsyncSearchList(ResultSet []clickhouse.QueryResultRow, typ model.AsyncSearchQueryType) ([]byte, error) {
	hits := make([]model.AsyncSearchHit, len(ResultSet))
	for i := range ResultSet {
		hits[i].Fields = make(map[string][]interface{})
		for _, col := range ResultSet[i].Cols {
			hits[i].Fields[col.ColName] = []interface{}{col.Value}
		}
	}

	var total *model.Total
	var id *string
	switch typ {
	case model.ListByField:
		total = &model.Total{
			Value:    len(ResultSet),
			Relation: "eq",
		}
	case model.ListAllFields:
		for i := range ResultSet {
			hits[i].ID = "fake-id"
			hits[i].Index = "index-TODO-insert-tablename-index-here"
			hits[i].Score = 1
			hits[i].Version = 1
			hits[i].Sort = []any{
				"2024-01-30T19:38:54.607Z",
				2944,
			}
			hits[i].Highlight = map[string][]string{}
		}
		id = new(string)
		*id = "fake-id"
	default:
		return nil, fmt.Errorf("unknown AsyncSearchListQueryType: %v", typ)
	}

	response := model.AsyncSearchEntireResp{
		Response: model.AsyncSearchResp{
			Hits: model.AsyncSearchHits{
				Total: total,
				Hits:  hits,
			},
		},
		ID: id,
	}
	return json.MarshalIndent(response, "", "  ")
}

func MakeResponseAsyncSearchQuery(ResultSet []clickhouse.QueryResultRow, typ model.AsyncSearchQueryType) ([]byte, error) {
	switch typ {
	case model.Histogram, model.AggsByField:
		return MakeResponseAsyncSearchAggregated(ResultSet, typ)
	case model.ListByField, model.ListAllFields:
		return MakeResponseAsyncSearchList(ResultSet, typ)
	default:
		return nil, fmt.Errorf("unknown AsyncSearchQueryType: %v", typ)
	}
}

// GetFieldsList
// TODO flatten tuples, I think (or just don't support them for now, we don't want them at the moment in production schemas)
func (cw *ClickhouseQueryTranslator) GetFieldsList(tableName string) []string {
	return []string{"message"}
}

func (cw *ClickhouseQueryTranslator) BuildSimpleSelectQuery(tableName, whereClause string) *model.Query {
	return &model.Query{
		Fields:      []string{"*"},
		WhereClause: whereClause,
		TableName:   tableName,
		CanParse:    true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildSimpleCountQuery(tableName, whereClause string) *model.Query {
	return &model.Query{
		NonSchemaFields: []string{"count()"},
		WhereClause:     whereClause,
		TableName:       tableName,
		CanParse:        true,
	}
}

// GetNMostRecentRows fieldName == "*" ==> we query all
// otherwise ==> only this 1 field
func (cw *ClickhouseQueryTranslator) BuildNMostRecentRowsQuery(tableName, fieldName, timestampFieldName, whereClause string, limit int) *model.Query {
	suffixClauses := make([]string, 0)
	if len(timestampFieldName) > 0 {
		suffixClauses = append(suffixClauses, "ORDER BY `"+timestampFieldName+"` DESC")
	}
	if limit > 0 {
		suffixClauses = append(suffixClauses, "LIMIT "+strconv.Itoa(limit))
	}
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{},
		WhereClause:     whereClause,
		SuffixClauses:   suffixClauses,
		TableName:       tableName,
		CanParse:        true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildHistogramQuery(tableName, timestampFieldName, whereClauseOriginal string) *model.Query {
	duration := 15 * time.Minute                                // TODO change this to be dynamic
	histogramOneBar := cw.durationToHistogramInterval(duration) // 1 bar duration
	groupByClause := "toInt64(toUnixTimestamp64Milli(`" + timestampFieldName + "`)/" + strconv.FormatInt(histogramOneBar.Milliseconds(), 10) + ")"
	whereClause := strconv.Quote(timestampFieldName) + ">=timestamp_sub(SECOND," + strconv.FormatInt(int64(duration.Seconds()), 10) + ", now64())"
	if len(whereClauseOriginal) > 0 {
		whereClause = "(" + whereClauseOriginal + ") AND (" + whereClause + ")"
	}
	return &model.Query{
		Fields:          []string{},
		NonSchemaFields: []string{groupByClause, "count()"},
		WhereClause:     whereClause,
		SuffixClauses:   []string{"GROUP BY " + groupByClause},
		TableName:       tableName,
		CanParse:        true,
	}
}

//lint:ignore U1000 Not used yet
func (cw *ClickhouseQueryTranslator) BuildAutocompleteSuggestionsQuery(tableName, fieldName string, prefix string, limit int) *model.Query {
	whereClause := ""
	if len(prefix) > 0 {
		whereClause = strconv.Quote(fieldName) + " iLIKE '" + prefix + "%'"
	}
	suffixClauses := make([]string, 0)
	if limit > 0 {
		suffixClauses = append(suffixClauses, "LIMIT "+strconv.Itoa(limit))
	}
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{},
		WhereClause:     whereClause,
		SuffixClauses:   suffixClauses,
		TableName:       tableName,
		CanParse:        true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildFacetsQuery(tableName, fieldName, whereClause string, limit int) *model.Query {
	suffixClauses := []string{"GROUP BY " + strconv.Quote(fieldName), "ORDER BY count() DESC"}
	_ = limit // we take all rows for now
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{"count()"},
		WhereClause:     whereClause,
		SuffixClauses:   suffixClauses,
		TableName:       tableName,
		CanParse:        true,
	}
}

// earliest == true  <==> we want earliest timestamp
// earliest == false <==> we want latest timestamp
func (cw *ClickhouseQueryTranslator) BuildTimestampQuery(tableName, timestampFieldName, whereClause string, earliest bool) *model.Query {
	var orderBy string
	if earliest {
		orderBy = "ORDER BY `" + timestampFieldName + "` ASC"
	} else {
		orderBy = "ORDER BY `" + timestampFieldName + "` DESC"
	}
	suffixClauses := []string{orderBy, "LIMIT 1"}
	return &model.Query{
		Fields:        []string{timestampFieldName},
		WhereClause:   whereClause,
		SuffixClauses: suffixClauses,
		TableName:     tableName,
		CanParse:      true,
	}
}

/*
		How Kibana shows histogram (how long one bar is):
	    query duration -> one histogram's bar ...
	    10s  -> 200ms
		14s  -> 280ms
		20s  -> 400ms
		24s  -> 480ms
		25s  -> 1s
		[25s, 4m]   -> 1s
		[5m, 6m]    -> 5s
		[7m, 12m]   -> 10s
		[13m, 37m]  -> 30s
		[38m, 140m] -> 1m
		[150m, 7h]  -> 5m
		[8h, 16h]   -> 10m
		[17h, 37h]  -> 30m
		[38h, 99h]  -> 1h
		[100h, 12d] -> 3h
		[13d, 49d]  -> 12h
		[50d, 340d] -> 1d
		[350d, 34m] -> 7d
		[35m, 15y]  -> 1m
*/

func (cw *ClickhouseQueryTranslator) durationToHistogramInterval(d time.Duration) time.Duration {
	switch {
	case d < 25*time.Second:
		ms := d.Milliseconds() / 50
		ms += 20 - (ms % 20)
		return time.Millisecond * time.Duration(ms)
	case d <= 4*time.Minute:
		return time.Second
	case d < 7*time.Minute:
		return 5 * time.Second
	case d < 13*time.Minute:
		return 10 * time.Second
	case d < 38*time.Minute:
		return 30 * time.Second
	case d <= 140*time.Minute:
		return time.Minute
	case d <= 7*time.Hour:
		return 5 * time.Minute
	case d <= 16*time.Hour:
		return 10 * time.Minute
	case d <= 37*time.Hour:
		return 30 * time.Minute
	case d <= 99*time.Hour:
		return time.Hour
	case d <= 12*24*time.Hour:
		return 3 * time.Hour
	case d <= 49*24*time.Hour:
		return 12 * time.Hour
	case d <= 340*24*time.Hour:
		return 24 * time.Hour
	case d <= 35*30*24*time.Hour:
		return 7 * 24 * time.Hour
	default:
		return 30 * 24 * time.Hour
	}
}
