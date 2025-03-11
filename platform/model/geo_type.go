// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

const QuesmaGeoLatFunction = "__quesma_geo_lat"
const QuesmaGeoLonFunction = "__quesma_geo_lon"

func NewGeoLat(propertyName string) Expr {
	return NewFunction(QuesmaGeoLatFunction, NewColumnRef(propertyName))
}

func NewGeoLon(propertyName string) Expr {
	return NewFunction(QuesmaGeoLonFunction, NewColumnRef(propertyName))
}
