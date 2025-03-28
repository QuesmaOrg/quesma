// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package transformations

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"strings"
)

// TODO it should be removed and transformations should be in 1 place.
// Only introduced temporarly e.g. for terms_enum to be able to get transformed
// (terms_enum package can't import frontend_conntectors package)
func ApplyAllNecessaryCommonTransformations(query *model.Query, schema schema.Schema, isFieldMapSyntaxEnabled bool) (*model.Query, error) {
	query, err := ApplyFieldMapSyntax(schema, query, isFieldMapSyntaxEnabled)
	if err != nil {
		return nil, err
	}
	return query, nil
}

func ApplyFieldMapSyntax(indexSchema schema.Schema, query *model.Query, isFieldMapSyntaxEnabled bool) (*model.Query, error) {
	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {

		// we don't want to resolve our well know technical fields
		if e.ColumnName == model.FullTextFieldNamePlaceHolder || e.ColumnName == common_table.IndexNameColumn {
			return e
		}
		// 1. we check if the field name point to the map
		if isFieldMapSyntaxEnabled {
			elements := strings.Split(e.ColumnName, ".")
			if len(elements) > 1 {
				if mapField, ok := indexSchema.ResolveField(elements[0]); ok {
					// check if we have map type, especially  Map(String, any) here
					if mapField.Type.Name == schema.QuesmaTypeMap.Name &&
						(strings.HasPrefix(mapField.InternalPropertyType, "Map(String") ||
							strings.HasPrefix(mapField.InternalPropertyType, "Map(LowCardinality(String")) {
						if len(elements) > 2 && (elements[len(elements)-1] == "keyword" || elements[len(elements)-1] == "text") {
							elements = elements[:len(elements)-1]
						}
						return model.NewFunction("arrayElement", model.NewColumnRef(elements[0]), model.NewLiteral(fmt.Sprintf("'%s'", strings.Join(elements[1:], "."))))
					}
				}
			}
		}
		return e
	}

	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}

	return query, nil
}
