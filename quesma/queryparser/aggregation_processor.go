package queryparser

import (
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
)

func (cw *ClickhouseQueryTranslator) processBucketAggregation(aggrBuilder *aggrQueryBuilder,
	queryMap QueryMap, resultAccumulator *[]model.Query) error {
	switch aggrType := aggrBuilder.Type.(type) {
	case bucket_aggregations.Range:
		cw.processRangeAggregation(aggrBuilder, aggrType, queryMap, resultAccumulator)
		return nil
	case bucket_aggregations.Filters:
		err := cw.processFiltersAggregation(aggrBuilder, aggrType, queryMap, resultAccumulator)
		//*resultAccumulator = append(*resultAccumulator, aggrBuilder.finishBuildingAggregationBucket())
		return err
	case bucket_aggregations.Terms:
		cw.processTermsAggregation(aggrBuilder, aggrType, queryMap)
	case bucket_aggregations.Histogram:
		cw.processHistogramAggregation(aggrBuilder, aggrType)
	}

	// common part for most aggregations

	if len(aggrBuilder.Aggregators) > 0 {
		aggrBuilder.Aggregators[len(aggrBuilder.Aggregators)-1].Empty = false
	} else {
		logger.ErrorWithCtx(cw.Ctx).Msgf("bucket aggregation with 0 aggregators, aggrBuilder: %+v", aggrBuilder)
	}
	fmt.Println("AGGR BUILDER COMMON: ", aggrBuilder)
	*resultAccumulator = append(*resultAccumulator, aggrBuilder.finishBuildingAggregationBucket())

	if _, isTerms := aggrBuilder.Type.(bucket_aggregations.Terms); isTerms {
		fmt.Printf("\n\nAAAAA blablabla %v\n\n", aggrBuilder)
		oldAgg := aggrBuilder
		aggrBuilder = aggrBuilder.clone()
		cw.postprocessTermsAggregation(aggrBuilder)
		fmt.Printf("\n\nBBBBB blablabla %v\n\n", oldAgg)
	}

	if aggs, exists := queryMap["aggs"]; exists {
		fmt.Println("wchodze do aggs:", aggs)
		err := cw.parseAggregationNames(aggrBuilder.clone(), aggs.(QueryMap), resultAccumulator)
		if err != nil {
			return err
		}
		delete(queryMap, "aggs")
	}

	return nil
}
