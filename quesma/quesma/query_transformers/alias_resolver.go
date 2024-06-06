package query_transformers

import (
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/where_clause"
	"mitmproxy/quesma/quesma/config"
)

// AliasResolver transforms the queries by applying aliases to the fields according to the configuration
type AliasResolver struct {
	indexConf map[string]config.IndexConfiguration
}

func NewAliasResolver(config map[string]config.IndexConfiguration) *AliasResolver {
	return &AliasResolver{indexConf: config}
}

func (ar *AliasResolver) Transform(queries []model.Query) (transformedQueries []model.Query, err error) {
	for _, query := range queries {
		transformedQueries = append(transformedQueries, *ar.applyAliases(&query))
	}
	return transformedQueries, nil
}

// ApplyAliases is effectively a no-op at this point as all the aliasing is resolved during parsing byt Table.ResolveField()
func (ar *AliasResolver) applyAliases(query *model.Query) *model.Query {
	if query.WhereClause == nil {
		return nil
	}

	if indexCfg, ok := ar.indexConf[query.FromClause]; ok { // this shall be table reference
		resolver := &where_clause.AliasResolver{IndexCfg: indexCfg}
		query.WhereClause.Accept(resolver)
		return query
	} else { // no aliases for fields this table configured
		return nil
	}
}
