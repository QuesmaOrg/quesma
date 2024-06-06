package quesma

import "mitmproxy/quesma/model"

type Transformer interface {
	Transform(queries []model.Query) ([]model.Query, error)
}

type TransformationPipeline struct {
	transformers []Transformer
}

func (o *TransformationPipeline) Transform(queries []model.Query) ([]model.Query, error) {
	for _, transformer := range o.transformers {
		queries, _ = transformer.Transform(queries)
	}
	return queries, nil
}
