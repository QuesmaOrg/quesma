// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

// Used in tests:

type StaticRegistry struct {
	Tables               map[TableName]Schema
	DynamicConfiguration map[string]Table
}

func (e StaticRegistry) AllSchemas() map[TableName]Schema {
	if e.Tables != nil {
		return e.Tables
	} else {
		return map[TableName]Schema{}
	}
}

func (e StaticRegistry) FindSchema(name TableName) (Schema, bool) {
	if e.Tables == nil {
		return Schema{}, false
	}
	s, found := e.Tables[name]
	return s, found
}

func (e StaticRegistry) UpdateDynamicConfiguration(name TableName, table Table) {
	e.DynamicConfiguration[name.AsString()] = table
}
