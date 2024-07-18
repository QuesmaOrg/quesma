// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"quesma/model"
	"quesma/quesma/config"
)

func makeSourceToDestMappings(indexMappings map[string]config.IndexMappingsConfiguration) map[string]string {
	sourceToDestMapping := make(map[string]string)
	for _, indexMapping := range indexMappings {
		for _, sourceIndex := range indexMapping.Mappings {
			destIndex := indexMapping.Name
			sourceToDestMapping[sourceIndex] = destIndex
		}
	}
	return sourceToDestMapping
}

func (s *SchemaCheckPass) applyIndexMappingTransformations(query *model.Query) (*model.Query, error) {
	sourceToDestMapping := makeSourceToDestMappings(s.indexMappings)

	visitor := model.NewBaseVisitor()

	// For now, we only rewrite the table refs
	// as it seems to be sufficient for the current use case
	// as there will be only one table ref in the query
	// we don't need to worry about the other expressions
	visitor.OverrideVisitTableRef = func(b *model.BaseExprVisitor, e model.TableRef) interface{} {
		if destIndex, ok := sourceToDestMapping[e.Name]; ok {
			return model.NewTableRef(destIndex)
		}
		return model.NewTableRef(e.Name)
	}
	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil

}
