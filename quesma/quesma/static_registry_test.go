// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import "quesma/schema"

type staticRegistry struct {
	tables map[schema.TableName]schema.Schema
}

func (e staticRegistry) AllSchemas() map[schema.TableName]schema.Schema {
	if e.tables != nil {
		return e.tables
	} else {
		return map[schema.TableName]schema.Schema{}
	}
}

func (e staticRegistry) FindSchema(name schema.TableName) (schema.Schema, bool) {
	if e.tables == nil {
		return schema.Schema{}, false
	}
	s, found := e.tables[name]
	return s, found
}
