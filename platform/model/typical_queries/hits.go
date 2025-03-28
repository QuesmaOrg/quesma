// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package typical_queries

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/k0kubun/pp"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Hits is a struct responsible for returning hits part of response.
// There's actually no such aggregation in Elastic.
//
// We still have a couple of distinct handlers for different types of requests, and Hits is one of them.
// We treat it here as if it was a normal aggregation, even though it's technically not completely correct.
// But it works, and because of that we can unify response creation part of Quesma, so it's very useful.
type Hits struct {
	ctx                context.Context
	table              *clickhouse.Table
	highlighter        *model.Highlighter
	sortFieldNames     []string
	addSource          bool // true <=> we add hit.Source field to the response
	addScore           bool // true <=> we add hit.Score field to the response (whose value is always 1)
	addVersion         bool // true <=> we add hit.Version field to the response (whose value is always 1)
	indexes            []string
	timestampFieldName string
}

func NewHits(ctx context.Context, table *clickhouse.Table, highlighter *model.Highlighter,
	sortFieldNames []string, addSource, addScore, addVersion bool, indexes []string) Hits {

	return Hits{ctx: ctx, table: table, highlighter: highlighter, sortFieldNames: sortFieldNames,
		addSource: addSource, addScore: addScore, addVersion: addVersion, indexes: indexes}
}

const (
	defaultScore   = 1 // if we add "score" field, it's always 1
	defaultVersion = 1 // if we  add "version" field, it's always 1
)

func (query Hits) AggregationType() model.AggregationType {
	return model.TypicalAggregation
}

func (query Hits) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {

	hits := make([]model.SearchHit, 0, len(rows))

	lookForCommonTableIndexColumn := true

	for i, row := range rows {

		// sane default
		indexName := query.indexes[0]

		// we don't look for common table index column if we didn't find it in the first row
		if lookForCommonTableIndexColumn {
			var found bool
			for _, cell := range row.Cols {
				if cell.ColName == common_table.IndexNameColumn {
					indexName = cell.Value.(string)
					found = true
					break
				}
			}
			if !found {
				lookForCommonTableIndexColumn = false
			}
		}

		hit := model.NewSearchHit(indexName)

		if query.addScore {
			hit.Score = defaultScore
		}
		if query.addVersion {
			hit.Version = defaultVersion
		}
		if query.addSource {
			hit.Source = []byte(rows[i].String(query.ctx))
		}
		query.addAndHighlightHit(&hit, &row)

		hit.ID = query.computeIdForDocument(hit, strconv.Itoa(i+1))
		for _, fieldName := range query.sortFieldNames {
			if val, ok := hit.Fields[fieldName]; ok {
				hit.Sort = append(hit.Sort, elasticsearch.FormatSortValue(val[0]))
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("field %s not found in fields", fieldName)
			}
		}
		hits = append(hits, hit)
	}

	return model.JsonMap{
		"hits": model.SearchHits{
			Total: &model.Total{
				Value:    len(rows),
				Relation: "eq", // TODO fix in next PR
			},
			Hits: hits,
		},
		"shards": model.ResponseShards{
			Total:      1,
			Successful: 1,
			Failed:     0,
		},
	}
}

func (query Hits) addAndHighlightHit(hit *model.SearchHit, resultRow *model.QueryResultRow) {
	toProperType := func(val interface{}) any {
		v := reflect.ValueOf(val)
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return val
		}

		result := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			result[i] = v.Index(i).Interface()
		}
		return result
	}

	for _, col := range resultRow.Cols {

		// skip internal columns
		if col.ColName == common_table.IndexNameColumn {
			continue
		}

		if col.Value == nil {
			continue // We don't return empty value
		}

		var mapAsValue bool
		suffixes, vals := []string{}, []any{}
		switch colT := col.Value.(type) {
		case map[string]*string:
			mapAsValue = true
			for key, value := range colT {
				if value != nil {
					suffixes = append(suffixes, "."+key)
					vals = append(vals, *value)
				}
			}
		case map[string]string:
			mapAsValue = true
			for key, value := range colT {
				suffixes = append(suffixes, "."+key)
				vals = append(vals, value)
			}
		default:
			suffixes = []string{""}
			vals = []any{colT}
		}

		pp.Println("suffixes", suffixes, "vals", vals)

		columnNameWithoutMapSuffix := col.ColName
		for i := 0; i < len(vals); i++ {
			columnName := columnNameWithoutMapSuffix + suffixes[i]
			hit.Fields[columnName] = append(hit.Fields[columnName], toProperType(vals[i]))

			var fieldName string
			if mapAsValue {
				fieldName = columnName
			} else {
				fieldName = util.FieldToColumnEncoder(columnName)
			}
			pp.Println("RRR", col, "column name", columnName, fieldName, toProperType(col.Value), "should high?", query.highlighter.ShouldHighlight(util.FieldToColumnEncoder(columnName)))
			// TODO using using util.FieldToColumnEncoder is a workaround
			// we first build highlighter tokens using internal representation
			// then we do postprocessing changing columns to public fields
			// and then highlighter build json using public one
			// which is incorrect
			if query.highlighter.ShouldHighlight(fieldName) {
				fmt.Println("TAK", vals[i])
				// check if we have a string here and if so, highlight it
				switch valueAsString := vals[i].(type) {
				case string:
					hit.Highlight[columnName] = query.highlighter.HighlightValue(fieldName, valueAsString)
				case *string:
					if valueAsString != nil {
						hit.Highlight[columnName] = query.highlighter.HighlightValue(fieldName, *valueAsString)
					}
				case []string:
					for _, v := range valueAsString {
						hit.Highlight[columnName] = append(hit.Highlight[columnName], query.highlighter.HighlightValue(fieldName, v)...)
					}
				case []*string:
					for _, v := range valueAsString {
						if v != nil {
							hit.Highlight[columnName] = append(hit.Highlight[columnName], query.highlighter.HighlightValue(fieldName, *v)...)
						}
					}
				default:
					logger.WarnWithCtx(query.ctx).Msgf("unknown type for hit highlighting: %T, value: %v", col.Value, col.Value)
				}
			}
		}
	}

	// TODO: highlight and field checks
	pp.Println("high", hit.Highlight, query.highlighter.Tokens)
	for fieldName, target := range query.table.Aliases() {
		fmt.Println("fieldName", fieldName, "target", target)
		if v, ok := hit.Fields[target]; ok {
			fmt.Println("ok")
			hit.Fields[fieldName] = v
		}
	}
}

func (query Hits) WithTimestampField(fieldName string) Hits {
	query.timestampFieldName = fieldName
	return query
}

func (query Hits) computeIdForDocument(doc model.SearchHit, defaultID string) string {

	if query.timestampFieldName == "" {
		return defaultID
	}

	tsFieldName := query.timestampFieldName

	var pseudoUniqueId string

	if v, ok := doc.Fields[tsFieldName]; ok {
		if vv, okk := v[0].(time.Time); okk {
			// At database level we only compare timestamps with millisecond precision
			// However in search results we append `q` plus generated digits (we use q because it's not in hex)
			// so that kibana can iterate over documents in UI
			pseudoUniqueId = fmt.Sprintf("%xq%s", vv, defaultID)
		} else {
			logger.WarnWithCtx(query.ctx).Msgf("failed to convert timestamp field [%v] to time.Time", v[0])
			return defaultID
		}
	}
	return pseudoUniqueId
}

func (query Hits) String() string {
	return fmt.Sprintf("hits(indexes: %v)", strings.Join(query.indexes, ", "))
}
