package quesma

import "mitmproxy/quesma/model"

type SchemaCheckPass struct {
}

func (s *SchemaCheckPass) Transform(queries []model.Query) ([]model.Query, error) {
	return queries, nil
}
