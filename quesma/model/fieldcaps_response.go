package model

// https://github.com/elastic/go-elasticsearch/blob/main/typedapi/core/fieldcaps/response.go#L35
type FieldCapsResponse struct {
	Fields  map[string]map[string]FieldCapability `json:"fields"`
	Indices []string                              `json:"indices"`
}
