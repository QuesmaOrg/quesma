// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

// StaticRegistry is an implementation of Registry interface MEANT TO BE USED ONLY IN TESTS
// This is due to the original schemaRegistry having a heavily side-effecting nature.
// In the future we might revisit this design - have schema being fed by external components and ditch this implementation.
type StaticRegistry struct {
	Tables               map[TableName]Schema
	DynamicConfiguration map[string]Table
	FieldEncodings       map[FieldEncodingKey]EncodedFieldName
}

func (e *StaticRegistry) AllSchemas() map[TableName]Schema {
	if e.Tables != nil {
		return e.Tables
	} else {
		return map[TableName]Schema{}
	}
}

func (e *StaticRegistry) FindSchema(name TableName) (Schema, bool) {
	if e.Tables == nil {
		return Schema{}, false
	}
	s, found := e.Tables[name]
	return s, found
}

func (e *StaticRegistry) UpdateDynamicConfiguration(name TableName, table Table) {
	e.DynamicConfiguration[name.AsString()] = table
}

func (e *StaticRegistry) UpdateFieldEncodings(encodings map[FieldEncodingKey]EncodedFieldName) {
	if e.FieldEncodings == nil {
		e.FieldEncodings = map[FieldEncodingKey]EncodedFieldName{}
	}
	for k, v := range encodings {
		e.FieldEncodings[k] = EncodedFieldName(v)
	}
}

func (e *StaticRegistry) GetFieldEncodings() map[FieldEncodingKey]EncodedFieldName {
	if e.FieldEncodings == nil {
		return map[FieldEncodingKey]EncodedFieldName{}
	}
	return e.FieldEncodings
}

func (e *StaticRegistry) UpdateFieldsOrigins(name TableName, fields map[FieldName]FieldSource) {

}
