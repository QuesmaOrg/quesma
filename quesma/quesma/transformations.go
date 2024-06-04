package quesma

import "mitmproxy/quesma/model"

type Transformer interface {
	Transform(query []model.Query) ([]model.Query, error)
}

type TransformationPipeline struct {
	transformers []Transformer
}

func (o *TransformationPipeline) Transform(query []model.Query) ([]model.Query, error) {
	for _, transformer := range o.transformers {
		query, _ = transformer.Transform(query)
	}
	return query, nil
}
