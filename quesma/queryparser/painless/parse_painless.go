// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painless

import "quesma/model"

func ParsePainlessScriptToExpr(s string) model.Expr {

	// TODO: add a real parser here
	if s == "emit(doc['timestamp'].value.getHour());" {
		return model.NewFunction(model.DateHourFunction, model.NewColumnRef(model.TimestampFieldName))
	}

	// harmless default
	return model.NewLiteral("NULL")
}
