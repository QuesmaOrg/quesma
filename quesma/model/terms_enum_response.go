package model

// TermsEnumResponse - copied from
// https://github.com/elastic/go-elasticsearch/blob/main/typedapi/core/termsenum/response.go
type TermsEnumResponse struct {
	Complete bool `json:"complete"`
	//Shards_  types.ShardStatistics `json:"_shards"`
	Terms []string `json:"terms"`
}
