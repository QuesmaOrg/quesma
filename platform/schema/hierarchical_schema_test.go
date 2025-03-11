// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_SchemaToHierarchicalSchema(t *testing.T) {
	s := NewSchemaWithAliases(map[FieldName]Field{
		"id":                     {PropertyName: "id", InternalPropertyName: "id", Type: QuesmaTypeInteger},
		"total_amount":           {PropertyName: "total_amount", InternalPropertyName: "total_amount", Type: QuesmaTypeFloat},
		"product.name":           {PropertyName: "product.name", InternalPropertyName: "product::name", Type: QuesmaTypeText},
		"product.product_id":     {PropertyName: "product.product_id", InternalPropertyName: "product::product_id", Type: QuesmaTypeInteger},
		"triple.nested.example1": {PropertyName: "triple.nested.example1", InternalPropertyName: "triple::nested::example1", Type: QuesmaTypeText},
		"triple.nested.example2": {PropertyName: "triple.nested.example2", InternalPropertyName: "triple::nested::example2", Type: QuesmaTypeKeyword},
	}, map[FieldName]FieldName{}, true, "", nil)

	hs := SchemaToHierarchicalSchema(&s)
	assert.Equal(t, "", hs.Name)
	assert.Nil(t, hs.Field)
	assert.Equal(t, 4, len(hs.Children)) // "id", "total_amount", "product", "triple"

	childrenNames := make([]string, 0)
	for _, child := range hs.Children {
		childrenNames = append(childrenNames, child.Name)
		if child.Name == "product" {
			assert.Equal(t, 2, len(child.Children))
			assert.True(t, child.Children[0].Name == "name" || child.Children[1].Name == "name", "Expected 'name' field in 'product'")
			assert.True(t, child.Children[0].Name == "product_id" || child.Children[1].Name == "product_id", "Expected 'product_id' field in 'product'")
		} else if child.Name == "triple" {
			assert.Equal(t, 1, len(child.Children))
			assert.Equal(t, "nested", child.Children[0].Name)
			assert.Equal(t, 2, len(child.Children[0].Children))
			assert.True(t, child.Children[0].Children[0].Name == "example1" || child.Children[0].Children[1].Name == "example1", "Expected 'example1' field in 'triple.nested'")
			assert.True(t, child.Children[0].Children[0].Name == "example2" || child.Children[0].Children[1].Name == "example2", "Expected 'example2' field in 'triple.nested'")
		} else {
			assert.NotNil(t, child.Field)
			assert.Equal(t, child.Field.PropertyName.AsString(), child.Name)
		}
	}
	assert.ElementsMatch(t, []string{"id", "total_amount", "product", "triple"}, childrenNames)
}
