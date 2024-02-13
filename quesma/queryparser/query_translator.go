package queryparser

import (
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"regexp"
	"strconv"
	"time"
)

type JsonMap = map[string]interface{}

type ClickhouseQueryTranslator struct {
	ClickhouseLM *clickhouse.LogManager
	TableName    string
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
			Total: &model.Total{
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
			Total: &model.Total{
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
		Response: model.SearchResp{
			Aggregations: aggregations,
			Hits: model.SearchHits{
				Hits: []model.SearchHit{}, // seems redundant, but can't remove this, created JSON won't match
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
	hits := make([]model.SearchHit, len(ResultSet))
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
		Response: model.SearchResp{
			Hits: model.SearchHits{
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
func (cw *ClickhouseQueryTranslator) BuildNMostRecentRowsQuery(fieldName, timestampFieldName, whereClause string, limit int) *model.Query {
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
		TableName:       cw.TableName,
		CanParse:        true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildHistogramQuery(timestampFieldName, whereClauseOriginal, fixedInterval string) (*model.Query, time.Duration) {
	duration, err := durationFromWhere(whereClauseOriginal)
	if err != nil {
		panic(err)
	}
	histogramOneBar, err := time.ParseDuration(fixedInterval)
	if err != nil {
		panic(err)
	}
	groupByClause := "toInt64(toUnixTimestamp64Milli(`" + timestampFieldName + "`)/" + strconv.FormatInt(histogramOneBar.Milliseconds(), 10) + ")"
	whereClause := strconv.Quote(timestampFieldName) + ">=timestamp_sub(SECOND," + strconv.FormatInt(int64(duration.Seconds()), 10) + ", now64())"
	if len(whereClauseOriginal) > 0 {
		whereClause = "(" + whereClauseOriginal + ") AND (" + whereClause + ")"
	}
	query := model.Query{
		Fields:          []string{},
		NonSchemaFields: []string{groupByClause, "count()"},
		WhereClause:     whereClause,
		SuffixClauses:   []string{"GROUP BY " + groupByClause},
		TableName:       cw.TableName,
		CanParse:        true,
	}
	return &query, duration
}

//lint:ignore U1000 Not used yet
func (cw *ClickhouseQueryTranslator) BuildAutocompleteSuggestionsQuery(fieldName string, prefix string, limit int) *model.Query {
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
		TableName:       cw.TableName,
		CanParse:        true,
	}
}

func (cw *ClickhouseQueryTranslator) BuildFacetsQuery(fieldName, whereClause string, limit int) *model.Query {
	suffixClauses := []string{"GROUP BY " + strconv.Quote(fieldName), "ORDER BY count() DESC"}
	_ = limit // we take all rows for now
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{"count()"},
		WhereClause:     whereClause,
		SuffixClauses:   suffixClauses,
		TableName:       cw.TableName,
		CanParse:        true,
	}
}

// earliest == true  <==> we want earliest timestamp
// earliest == false <==> we want latest timestamp
func (cw *ClickhouseQueryTranslator) BuildTimestampQuery(timestampFieldName, whereClause string, earliest bool) *model.Query {
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
		TableName:     cw.TableName,
		CanParse:      true,
	}
}

var fromRegexp = regexp.MustCompile(`>=?parseDateTime64BestEffort\('([^']+)'\)`)
var toRegexp = regexp.MustCompile(`<=?parseDateTime64BestEffort\('([^']+)'\)`)

func durationFromWhere(input string) (time.Duration, error) {
	from := fromRegexp.FindAllStringSubmatch(input, -1)[0]
	to := toRegexp.FindAllStringSubmatch(input, -1)[0]

	startTime, err := time.Parse(time.RFC3339, from[1])
	if err != nil {
		return 0, err
	}

	endTime, err := time.Parse(time.RFC3339, to[1])
	if err != nil {
		return 0, err
	}

	return endTime.Sub(startTime), nil
}
