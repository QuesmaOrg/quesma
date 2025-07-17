// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package typical_queries

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/k0kubun/pp"
	"reflect"
	"sort"
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
	table              *database_common.Table
	highlighter        *model.Highlighter
	sortFieldNames     []string
	addSource          bool // true <=> we add hit.Source field to the response
	addScore           bool // true <=> we add hit.Score field to the response (whose value is always 1)
	addVersion         bool // true <=> we add hit.Version field to the response (whose value is always 1)
	indexes            []string
	timestampFieldName string
}

func NewHits(ctx context.Context, table *database_common.Table, highlighter *model.Highlighter,
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

	logger.Warn().Msgf("Query Hits: %v", rows)

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
		query.addAndHighlightHit(&hit, &row)

		hit.ID = query.computeIdForDocument(hit, strconv.Itoa(i+1))
		for _, fieldName := range query.sortFieldNames {
			if val, ok := hit.Fields[fieldName]; ok {
				hit.Sort = append(hit.Sort, elasticsearch.FormatSortValue(val[0]))
			} else if fieldName == "_doc" { // Kibana adds _doc as a tiebreaker field for sorting
				hit.Sort = append(hit.Sort, hit.ID)
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("field %s not found in fields", fieldName)
			}
		}

		// removeEmptyStringFields should be optional
		// (@trzysiek) I think it's best to not enable it by default, but only when
		// there's flag for it set in config to true
		//
		// If it's enabled, it should be at the end of our translation,
		// to not have to worry about e.g. sort working properly
		const removeEmptyStringFieldsInHitsQuery = true
		if removeEmptyStringFieldsInHitsQuery {
			query.removeEmptyStringFields(&hit)
		}

		// `Source` should be filled at the end, as surprisingly Kibana displays hits
		// from `Source` field, and not from `Fields`.
		// So e.g. if we want to filter out empty strings, like above, we need to fill `Source` afterwards.
		if query.addSource {
			hit.Source = []byte(query.hitToString(&row, &hit))
		}

		fmt.Println("&&& po remove2: ", hit.Fields)
		hits = append(hits, hit)
	}

	pp.Println(hits)

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
		fmt.Println("KK toProperType1 val:", val, "v: ", v)
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return val
		}
		fmt.Println("LL toProperType1 val:", val, "v: ", v)
		resultArray := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			resultArray[i] = v.Index(i).Interface()
		}
		return resultArray
	}

	for _, col := range resultRow.Cols {

		// skip internal columns
		if col.ColName == common_table.IndexNameColumn {
			continue
		}

		// we don't return empty value
		if col.Value == nil {
			continue
		}

		// Arrays below introduced only to unify handling for maps and other types.
		// If it's not a map, suffixes will always simply be [""] (no suffix) and vals will always be [col.Value]
		suffixes, vals := make([]string, 0), make([]any, 0)
		var isValueAMap bool

		switch colT := col.Value.(type) {
		case map[string]*string:
			isValueAMap = true
			for key, value := range colT {
				if value != nil {
					suffixes = append(suffixes, "."+key)
					vals = append(vals, *value)
				}
			}
		case map[string]string:
			isValueAMap = true
			for key, value := range colT {
				suffixes = append(suffixes, "."+key)
				vals = append(vals, value)
			}
		default:
			suffixes = []string{""}
			vals = []any{colT}
		}

		columnNameWithoutMapSuffix := col.ColName
		for i := 0; i < len(vals); i++ {
			columnName := columnNameWithoutMapSuffix + suffixes[i]
			hit.Fields[columnName] = append(hit.Fields[columnName], toProperType(vals[i]))

			var fieldName string
			if isValueAMap {
				fieldName = columnName // we don't decode, leave "map.key" as is
			} else {
				fieldName = util.FieldToColumnEncoder(columnName)
			}

			// TODO using using util.FieldToColumnEncoder is a workaround
			// we first build highlighter tokens using internal representation
			// then we do postprocessing changing columns to public fields
			// and then highlighter build json using public one
			// which is incorrect
			if query.highlighter.ShouldHighlight(fieldName) {
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
	for fieldName, target := range query.table.Aliases() {
		if v, ok := hit.Fields[target]; ok {
			hit.Fields[fieldName] = v
		}
	}

	fmt.Println("--------------- koniec: ", hit.Fields)
}

func (query Hits) removeEmptyStringFields(hit *model.SearchHit) {
	fieldNamesToRemove := make([]string, 0)
	for name, val := range hit.Fields {
		logger.Error().Msgf("KK hit field 1: %v, %v len(val): %v", name, val, len(val))
		// we only look for simple String/*String fields, so len == 1
		if len(val) != 1 {
			continue
		}

		fmt.Printf("Typ: %T\n", val[0])

		switch valT := val[0].(type) {
		case string:
			fmt.Println("???", valT == "")
			if valT == "" {
				fieldNamesToRemove = append(fieldNamesToRemove, name)
			}
		case *string:
			if valT != nil && *valT == "" {
				fieldNamesToRemove = append(fieldNamesToRemove, name)
			}
		}
	}

	for _, name := range fieldNamesToRemove {
		fmt.Println("=== Removing ", name)
		delete(hit.Fields, name)
	}
	fmt.Println("--- Po removing: ", hit.Fields)
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
			// However in search results we append `qqq` plus generated hash of the source to hex-encoded timestamp
			sourceHash := fmt.Sprintf("%x", ComputeHash(doc.Source))
			pseudoUniqueId = fmt.Sprintf("%xqqq%x", vv, sourceHash)
		} else {
			logger.WarnWithCtx(query.ctx).Msgf("failed to convert timestamp field [%v] to time.Time", v[0])
			return defaultID
		}
	}
	return pseudoUniqueId
}

func ComputeHash(data json.RawMessage) string {
	var parsed interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		hash := sha256.Sum256(data)
		return hex.EncodeToString(hash[:])
	}
	normalized := normalizeJSON(parsed)
	normalizedBytes, err := json.Marshal(normalized)
	if err != nil {
		hash := sha256.Sum256(data)
		return hex.EncodeToString(hash[:])
	}
	hash := sha256.Sum256(normalizedBytes)
	return hex.EncodeToString(hash[:])
}

// normalizeJSON recursively normalizes JSON structure to ensure consistent ordering for further hashing.
func normalizeJSON(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		normalized := make(map[string]interface{})
		for _, k := range keys {
			normalized[k] = normalizeJSON(val[k])
		}
		return normalized

	case []interface{}:
		normalized := make([]interface{}, len(val))
		for i, v := range val {
			normalized[i] = normalizeJSON(v)
		}
		return normalized

	default:
		return val
	}
}

// More or less copy of: func (r *QueryResultRow) String(ctx context.Context) string
// Some columns might already be excluded/removed, so we need a second implementation
func (query Hits) hitToString(row *model.QueryResultRow, hit *model.SearchHit) string {
	str := strings.Builder{}
	str.WriteString(util.Indent(1) + "{\n")
	i := 0
	for _, col := range row.Cols {
		// skip internal columns
		if col.ColName == common_table.IndexNameColumn {
			continue
		}

		// skip excluded fields
		if _, exists := hit.Fields[col.ColName]; !exists {
			continue
		}

		colStr := col.String(query.ctx)
		if len(colStr) > 0 {
			if i > 0 {
				str.WriteString(",\n")
			}
			str.WriteString(util.Indent(2) + colStr)
			i++
		}
	}
	str.WriteString("\n" + util.Indent(1) + "}")
	return str.String()
}

func (query Hits) String() string {
	return fmt.Sprintf("hits(indexes: %v)", strings.Join(query.indexes, ", "))
}
