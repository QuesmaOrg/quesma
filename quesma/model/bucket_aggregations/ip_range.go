// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"reflect"
)

// BiggestIpv4 is "255.255.255.255 + 1", so to say. Used in Elastic, because it always uses exclusive upper bounds.
// So instead of "<= 255.255.255.255", it uses "< ::1:0:0:0"
const BiggestIpv4 = "::1:0:0:0"

// Current limitation: we expect Clickhouse field to be IPv4 (and not IPv6)

// Clickhouse table to test SQLs:
// CREATE TABLE __quesma_table_name (clientip IPv4) ENGINE=Log
// INSERT INTO __quesma_table_name VALUES ('0.0.0.0'), ('5.5.5.5'), ('90.180.90.180'), ('128.200.0.8'),  ('192.168.1.67'), ('222.168.22.67')

// TODO make part of QueryType interface and implement for all aggregations
// TODO add bad requests to tests
// Doing so will ensure we see 100% of what we're interested in in our logs (now we see ~95%)
func CheckParamsIpRange(ctx context.Context, paramsRaw any) error {
	requiredParams := map[string]string{
		"field":  "string",
		"ranges": "map_todo_improve_this_check", // TODO should add same type check to this 'ranges' field, will be fixed
	}
	optionalParams := map[string]string{
		"keyed": "bool",
	}

	params, ok := paramsRaw.(model.JsonMap)
	if !ok {
		return fmt.Errorf("params is not a map, but %+v", paramsRaw)
	}

	// check if required are present
	for paramName, paramType := range requiredParams {
		paramVal, exists := params[paramName]
		if !exists {
			return fmt.Errorf("required parameter %s not found in params", paramName)
		}
		if paramType == "map_todo_improve_this_check" {
			continue // uncontinue after TODO is fixed
		}
		if reflect.TypeOf(paramVal).Name() != paramType { // TODO I'll make a small rewrite to not use reflect here
			return fmt.Errorf("required parameter %s is not of type %s, but %T", paramName, paramType, paramVal)
		}
	}

	// check if only required/optional are present
	for paramName := range params {
		if _, isRequired := requiredParams[paramName]; !isRequired {
			wantedType, isOptional := optionalParams[paramName]
			if !isOptional {
				return fmt.Errorf("unexpected parameter %s found in IP Range params %v", paramName, params)
			}
			if reflect.TypeOf(params[paramName]).Name() != wantedType { // TODO I'll make a small rewrite to not use reflect here
				return fmt.Errorf("optional parameter %s is not of type %s, but %T", paramName, wantedType, params[paramName])
			}
		}
	}

	return nil
}

type (
	IpRange struct {
		ctx       context.Context
		field     model.Expr
		intervals []IpInterval
		keyed     bool
	}
	IpInterval struct {
		begin string
		end   string
		key   *string // when nil, key is not present
	}
)

func NewIpRange(ctx context.Context, intervals []IpInterval, field model.Expr, keyed bool) *IpRange {
	return &IpRange{
		ctx:       ctx,
		field:     field,
		intervals: intervals,
		keyed:     keyed,
	}
}

func NewIpInterval(begin, end string, key *string) IpInterval {
	return IpInterval{begin: begin, end: end, key: key}
}

func (interval IpInterval) ToWhereClause(field model.Expr) model.Expr {
	isBegin := interval.begin != UnboundedInterval
	isEnd := interval.end != UnboundedInterval && interval.end != BiggestIpv4

	begin := model.NewInfixExpr(field, ">=", model.NewLiteralSingleQuoted(interval.begin))
	end := model.NewInfixExpr(field, "<", model.NewLiteralSingleQuoted(interval.end))

	if isBegin && isEnd {
		return model.NewInfixExpr(begin, "AND", end)
	} else if isBegin {
		return begin
	} else if isEnd {
		return end
	} else {
		return model.TrueExpr
	}
}

// String returns key part of the response, e.g. "1.0-2.0", or "*-6.55"
func (interval IpInterval) String() string {
	if interval.key != nil {
		return *interval.key
	}
	return fmt.Sprintf("%s-%s", interval.begin, interval.end)
}

func (query *IpRange) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query *IpRange) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	return nil
}

func (query *IpRange) String() string {
	return "ip_range"
}

func (query *IpRange) DoesNotHaveGroupBy() bool {
	return true
}

func (query *IpRange) CombinatorGroups() (result []CombinatorGroup) {
	for intervalIdx, interval := range query.intervals {
		prefix := fmt.Sprintf("range_%d__", intervalIdx)
		if len(query.intervals) == 1 {
			prefix = ""
		}
		result = append(result, CombinatorGroup{
			idx:         intervalIdx,
			Prefix:      prefix,
			Key:         interval.String(),
			WhereClause: interval.ToWhereClause(query.field),
		})
	}
	return
}

// bad requests: both to/from and mask

func (query *IpRange) CombinatorTranslateSqlResponseToJson(subGroup CombinatorGroup, rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 || len(rows[0].Cols) == 0 {
		logger.ErrorWithCtx(query.ctx).Msgf("need at least one row and column in ip_range aggregation response, rows: %d, cols: %d", len(rows), len(rows[0].Cols))
		return model.JsonMap{}
	}
	count := rows[0].Cols[len(rows[0].Cols)-1].Value
	response := model.JsonMap{
		"key":       subGroup.Key,
		"doc_count": count,
	}

	interval := query.intervals[subGroup.idx]
	if interval.begin != UnboundedInterval {
		response["from"] = interval.begin
	}
	if interval.end != UnboundedInterval {
		response["to"] = interval.end
	}

	return response
}

func (query *IpRange) CombinatorSplit() []model.QueryType {
	result := make([]model.QueryType, 0, len(query.intervals))
	for _, interval := range query.intervals {
		result = append(result, NewIpRange(query.ctx, []IpInterval{interval}, query.field, query.keyed))
	}
	return result
}
