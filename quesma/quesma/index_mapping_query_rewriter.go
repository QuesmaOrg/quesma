// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"quesma/model"
)

func (s *SchemaCheckPass) applyIndexMappingTransformations(query *model.Query) (*model.Query, error) {
	s.sourceToDestMapping = make(map[string]string)
	for _, indexMapping := range s.indexMappings {
		for _, sourceIndex := range indexMapping.Mappings {
			destIndex := indexMapping.Name
			s.sourceToDestMapping[sourceIndex] = destIndex
		}
	}
	visitor := model.NewBaseVisitor()

	// For now, we only rewrite the table refs
	// as it seems to be sufficient for the current use case
	visitor.OverrideVisitTableRef = func(b *model.BaseExprVisitor, e model.TableRef) interface{} {
		if destIndex, ok := s.sourceToDestMapping[e.Name]; ok {
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
