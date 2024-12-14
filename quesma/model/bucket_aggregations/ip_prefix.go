// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"math/big"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
	"reflect"
)

// Testing helpers:
// * (ipv4) Clickhouse table to test SQLs:
//   CREATE TABLE __quesma_table_name (clientip IPv4) ENGINE=Log
//   INSERT INTO __quesma_table_name VALUES ('0.0.0.0'), ('5.5.5.5'), ('90.180.90.180'), ('128.200.0.8'),  ('192.168.1.67'), ('222.168.22.67')
// * (ipv6) If ip field in Clickhouse is string, not IPv6, just change "ip_fieldname" to "ip_fieldname"::IPv6, to test SQLs from tests
//   (careful with that, most of the time it works, but sometimes some differences arise, I guess from big/little endian differences)

// TODO make part of QueryType interface and implement for all aggregations
// TODO add bad requests to tests
// Doing so will ensure we see 100% of what we're interested in in our logs (now we see ~95%)
func CheckParamsIpPrefix(ctx context.Context, paramsRaw any) error {
	requiredParams := map[string]string{
		"field":         "string",
		"prefix_length": "float64", // TODO should be int, will be fixed
	}
	optionalParams := map[string]string{
		"is_ipv6":              "bool",
		"append_prefix_length": "bool",
		"keyed":                "bool",
		"min_doc_count":        "int",
	}
	logIfYouSeeThemParams := []string{"min_doc_count"} // we don't use min_doc_count yet.

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
		if reflect.TypeOf(paramVal).Name() != paramType { // TODO I'll make a small rewrite to not use reflect here
			return fmt.Errorf("required parameter %s is not of type %s, but %T", paramName, paramType, paramVal)
		}
	}
	// prefixLength must be [0, 32] for ipv4, [0, 128] for ipv6
	prefixLength := params["prefix_length"].(float64) // will never panic because of checks above
	upperBound := 32.0
	if ipv6, exists := params["is_ipv6"]; exists && ipv6.(bool) {
		upperBound = 128.0
	}
	if util.IsSmaller(prefixLength, 0) || util.IsSmaller(upperBound, prefixLength) {
		return fmt.Errorf("prefix_length must be in range [0, %d], but got %f", int(upperBound), prefixLength)
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

	// log if you see them
	for _, warnParam := range logIfYouSeeThemParams {
		if _, exists := params[warnParam]; exists {
			logger.WarnWithCtxAndThrottling(ctx, "ip_prefix", warnParam, "we didn't expect %s in IP Range params %v", warnParam, params)
		}
	}

	return nil
}

type IpPrefix struct {
	ctx                context.Context
	field              model.Expr
	prefixLength       int
	isIpv6             bool
	appendPrefixLength bool
	keyed              bool
	minDocCount        int
}

func NewIpPrefix(ctx context.Context, field model.Expr, prefixLength int, isIpv6 bool, appendPrefixLength bool, keyed bool, minDocCount int) *IpPrefix {
	return &IpPrefix{
		ctx:                ctx,
		field:              field,
		prefixLength:       prefixLength,
		isIpv6:             isIpv6,
		appendPrefixLength: appendPrefixLength,
		keyed:              keyed,
		minDocCount:        minDocCount,
	}
}

func (query *IpPrefix) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query *IpPrefix) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	fmt.Println(rows)
	var (
		ok                      bool
		key, netmask, keySuffix string
		originalKeyIpv4         uint32  // if is_ipv6 is false, Clickhouse will always return uint32 as the key
		originalKeyIpv6         big.Int // if is_ipv6 is true, Clickhouse will always return big.Int as the key
	)
	if !query.isIpv6 {
		netmask = query.calcNetMask()
	}
	if query.appendPrefixLength {
		keySuffix = fmt.Sprintf("/%d", query.prefixLength)
	}
	buckets := make([]model.JsonMap, 0, len(rows))
	for _, row := range rows {
		var docCount any
		if query.prefixLength == 0 {
			if len(row.Cols) != 1 {
				logger.ErrorWithCtx(query.ctx).Msgf(
					"unexpected number of columns in ip_prefix aggregation response, len: %d, row: %v", len(row.Cols), row)
				continue
			}
			docCount = row.Cols[0].Value
		} else {
			if len(row.Cols) != 2 {
				logger.ErrorWithCtx(query.ctx).Msgf(
					"unexpected number of columns in ip_prefix aggregation response, len: %d, row: %v", len(row.Cols), row)
				continue
			}

			docCount = row.Cols[1].Value
			if query.isIpv6 {
				fmt.Println("LOL")
				originalKeyIpv6, ok = row.Cols[0].Value.(big.Int) // check if *big.Int or not
			} else {
				originalKeyIpv4, ok = row.Cols[0].Value.(uint32)
			}
			if !ok {
				logger.ErrorWithCtx(query.ctx).Msgf("unexpected type of key in ip_prefix aggregation response, got %T", row.Cols[0])
				continue
			}
		}

		if query.isIpv6 {
			key = query.calcKeyIPv6(originalKeyIpv6) + keySuffix
		} else {
			key = query.calcKeyIPv4(originalKeyIpv4) + keySuffix
		}
		bucket := model.JsonMap{
			"key":           key,
			"doc_count":     docCount,
			"prefix_length": query.prefixLength,
			"is_ipv6":       query.isIpv6,
		}
		if !query.isIpv6 {
			bucket["netmask"] = netmask
		}
		buckets = append(buckets, bucket)
	}

	fmt.Println(buckets)
	// usual case
	if !query.keyed {
		return model.JsonMap{
			"buckets": buckets,
		}
	}

	// unusual case: transform result buckets a bit
	keyedBuckets := make(model.JsonMap, len(buckets))
	for _, bucket := range buckets {
		key := bucket["key"].(string)
		delete(bucket, "key")
		keyedBuckets[key] = bucket
	}
	return model.JsonMap{
		"buckets": keyedBuckets,
	}
}

func (query *IpPrefix) String() string {
	return "ip_prefix"
}

// SqlSelectQuery returns the SQL query: intDiv(ip_field, some_power_of_2)
// ipv4 only for now
func (query *IpPrefix) SqlSelectQuery() model.Expr {
	if query.prefixLength == 0 {
		return nil
	}
	return model.NewFunction("intDiv", query.field, model.NewLiteral(query.divideByToGroupBy()))
}

func (query *IpPrefix) divideByToGroupBy() uint64 {
	if query.isIpv6 {
		return 1 << (128 - query.prefixLength)
	} else {
		return 1 << (32 - query.prefixLength)
	}
}

func (query *IpPrefix) calcKeyIPv4(originalKey uint32) string {
	if query.prefixLength == 0 {
		return "0.0.0.0"
	}
	ipAsInt := originalKey * uint32(query.divideByToGroupBy())
	part4 := ipAsInt % 256
	ipAsInt /= 256
	part3 := ipAsInt % 256
	ipAsInt /= 256
	part2 := ipAsInt % 256
	ipAsInt /= 256
	part1 := ipAsInt % 256
	return fmt.Sprintf("%d.%d.%d.%d", part1, part2, part3, part4)
}

func (query *IpPrefix) calcKeyIPv6(originalKey big.Int) string {
	// ipAsInt := originalKey * 2^(128-prefixLength)
	ipAsInt := originalKey.Mul(&originalKey, big.NewInt(1).Lsh(big.NewInt(1), uint(128-query.prefixLength)))
	return util.BigIntToIpv6(*ipAsInt)
}

// calcNetMask is only called for ipv4, so 1<<(query.prefixLength-1) will never overflow
func (query *IpPrefix) calcNetMask() string {
	if query.prefixLength == 0 {
		return "0.0.0.0"
	}
	biggestPossibleKey := uint32(1<<query.prefixLength - 1)
	return query.calcKeyIPv4(biggestPossibleKey) // netmask is the same as ip of biggest possible key
}
