package queryparser

import (
	"github.com/k0kubun/pp"
	"quesma/model"
	"quesma/plugins"
)

// maybe move to separate package?

type QueryOptimizationPipeline struct {
	transformers []plugins.QueryTransformer
}

type MergeMetricsAggsTransformer struct{}

func (t MergeMetricsAggsTransformer) Transform(queries []*model.Query) ([]*model.Query, error) {
	for _, q := range queries {
		pp.Println(q)
	}
	return queries, nil
}
