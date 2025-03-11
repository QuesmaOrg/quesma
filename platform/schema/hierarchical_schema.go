// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

type SchemaTreeNode struct {
	Name string // for example: "products" or "manufacturer" for "products.manufacturer", "" for root

	// Either this is a leaf node (non-nil field) or a branch node (non-empty children)
	Field    *Field
	Children []*SchemaTreeNode
}

func addToTree(root *SchemaTreeNode, field *Field) {
	components := field.PropertyName.Components()

	leafComponent := components[len(components)-1]
	components = components[:len(components)-1]

	currentNode := root
	for _, component := range components {
		found := false
		for _, child := range currentNode.Children {
			if child.Name == component {
				currentNode = child
				found = true
				break
			}
		}
		if !found {
			newNode := SchemaTreeNode{Name: component}
			currentNode.Children = append(currentNode.Children, &newNode)
			currentNode = &newNode
		}
	}

	currentNode.Children = append(currentNode.Children, &SchemaTreeNode{Name: leafComponent, Field: field})
}

// SchemaToHierarchicalSchema unflattens a "flat" Schema to a tree representation (returns a pointer to the root SchemaTreeNode).
//
// For example, if a field "product" with subfields "product_id" and "base_price", it's represented as two flat fields
// "product.product_id", "product.base_price" in Schema. This function will convert it to a tree with a root node "",
// a child node "product" with two grandchildren "product_id" and "base_price".
func SchemaToHierarchicalSchema(s *Schema) *SchemaTreeNode {
	root := SchemaTreeNode{Name: ""}

	for _, field := range s.Fields {
		addToTree(&root, &field)
	}

	return &root
}
