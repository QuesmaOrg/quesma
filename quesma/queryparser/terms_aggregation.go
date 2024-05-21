package queryparser

import (
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"strconv"
)

func (cw *ClickhouseQueryTranslator) parseTermsAggregation(queryMap QueryMap) (
	success bool, aggregation model.QueryType, err error) {
	for _, termsType := range []string{"terms", "significant_terms"} {
		termsRaw, exists := queryMap[termsType]
		if !exists {
			continue
		}
		terms, ok := termsRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("%s aggregation is not a map, terms: %+v. Skipping", termsType, termsRaw)
			continue
		}

		size := bucket_aggregations.DefaultSize
		if sizeRaw, exists := terms["size"]; exists {
			if sizeFloat, ok := sizeRaw.(float64); ok {
				size = int(sizeFloat)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("size in %s aggregation is not a number, size: %+v. Using default size", termsType, sizeRaw)
			}
		}
		quotedFieldName := strconv.Quote(cw.parseFieldField(terms, termsType))
		aggregation = bucket_aggregations.NewTerms(cw.Ctx, size, quotedFieldName, termsType == "significant_terms")

		delete(queryMap, termsType)
		return true, aggregation, nil
	}
	return false, nil, nil
}

func (cw *ClickhouseQueryTranslator) processTermsAggregation(aggrBuilder *aggrQueryBuilder, terms bucket_aggregations.Terms, queryMap QueryMap) {
	isEmptyGroupBy := len(aggrBuilder.GroupByFields) == 0
	aggrBuilder.GroupByFields = append(aggrBuilder.GroupByFields, terms.QuotedFieldName)
	aggrBuilder.NonSchemaFields = append(aggrBuilder.NonSchemaFields, terms.QuotedFieldName)
	if _, ok := queryMap["aggs"]; isEmptyGroupBy && !ok { // we can do limit only it terms are not nested
		aggrBuilder.OrderBy = append(aggrBuilder.OrderBy, "count() DESC")
		aggrBuilder.SuffixClauses = append(aggrBuilder.SuffixClauses, fmt.Sprintf("LIMIT %d", terms.Size))
	} else {
		aggrBuilder.OrderBy = append(aggrBuilder.OrderBy, terms.QuotedFieldName) // it's incorrect, but old logic had it
	}
	/* new logic, keep old until it's ready
	aggrBuilder.termsNr++

	pp.Println("--------------------", aggrBuilder.SuffixClauses, "terms nr:", aggrBuilder.termsNr)
	if len(aggrBuilder.GroupByFields) == 0 {
		aggrBuilder.SuffixClauses = []string{fmt.Sprintf("LIMIT %d", terms.Size)}
	} else {
		aggrBuilder.SuffixClauses = []string{fmt.Sprintf("LIMIT %d BY %s", terms.Size, strings.Join(aggrBuilder.GroupByFields, ", "))}
	}
	fmt.Println("GROUP: ", aggrBuilder.GroupByFields, terms.Size, aggrBuilder.SuffixClauses[len(aggrBuilder.SuffixClauses)-1], "sub: none")
	// nr := len(aggrBuilder.SubQueries)
	quotedFieldName := strconv.Quote(terms.FieldName)
	aggrBuilder.GroupByFields = append(aggrBuilder.GroupByFields, quotedFieldName)
	aggrBuilder.OrderBy = append(aggrBuilder.OrderBy, "count() desc")
	aggrBuilder.OrderBy = append(aggrBuilder.OrderBy, quotedFieldName)
	aggrBuilder.Fields = append(aggrBuilder.Fields, terms.FieldName)
	// fmt.Println("=== Dodaje termsy SubQuery: ", aggrBuilder.SubQueries, "termsNr: ", aggrBuilder.termsNr, "len(suffix)", len(aggrBuilder.SuffixClauses))
	*/
}

func (cw *ClickhouseQueryTranslator) postprocessTermsAggregation(aggrBuilder *aggrQueryBuilder) {
	// aggrBuilder.Query.AddSubQueryFromCurrentState(cw.Ctx, aggrBuilder.termsNr)
	// aggrBuilder.Query.ReplaceLastOrderByWithSubQuery(aggrBuilder.termsNr)
}
