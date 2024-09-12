package model

const QuesmaGeoLatFunction = "__quesma_geo_lat"
const QuesmaGeoLonFunction = "__quesma_geo_lon"

func NewGeoLat(propertyName string) Expr {
	return NewFunction(QuesmaGeoLatFunction, NewColumnRef(propertyName))
}

func NewGeoLon(propertyName string) Expr {
	return NewFunction(QuesmaGeoLonFunction, NewColumnRef(propertyName))
}
