// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// TermsEnumResponse - copied from
// https://github.com/elastic/go-elasticsearch/blob/main/typedapi/core/termsenum/response.go
type TermsEnumResponse struct {
	Complete bool `json:"complete"`
	//Shards_  types.ShardStatistics `json:"_shards"`
	Terms []string `json:"terms"`
}
