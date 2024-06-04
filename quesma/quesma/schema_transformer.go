package quesma

import "mitmproxy/quesma/model"

type SchemaCheckPass struct {
}

func (s *SchemaCheckPass) Transform(query []model.Query) ([]model.Query, error) {
	return query, nil
}
