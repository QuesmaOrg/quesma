// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/v2/core/types"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reflect"
	"strings"
	"testing"
)

var (
	kibanaSampleFlightsFields = map[string]schema.Column{
		"AvgTicketPrice":     {Name: "AvgTicketPrice", Type: "float"},
		"Cancelled":          {Name: "Cancelled", Type: "boolean"},
		"Carrier":            {Name: "Carrier", Type: "keyword"},
		"Dest":               {Name: "Dest", Type: "keyword"},
		"DestAirportID":      {Name: "DestAirportID", Type: "keyword"},
		"DestCityName":       {Name: "DestCityName", Type: "keyword"},
		"DestCountry":        {Name: "DestCountry", Type: "keyword"},
		"DestLocation":       {Name: "DestLocation", Type: "point"},
		"DestRegion":         {Name: "DestRegion", Type: "keyword"},
		"DestWeather":        {Name: "DestWeather", Type: "keyword"},
		"DistanceKilometers": {Name: "DistanceKilometers", Type: "float"},
		"DistanceMiles":      {Name: "DistanceMiles", Type: "float"},
		"FlightDelay":        {Name: "FlightDelay", Type: "boolean"},
		"FlightDelayMin":     {Name: "FlightDelayMin", Type: "long"},
		"FlightDelayType":    {Name: "FlightDelayType", Type: "keyword"},
		"FlightNum":          {Name: "FlightNum", Type: "keyword"},
		"FlightTimeHour":     {Name: "FlightTimeHour", Type: "keyword"},
		"FlightTimeMin":      {Name: "FlightTimeMin", Type: "float"},
		"Origin":             {Name: "Origin", Type: "keyword"},
		"OriginAirportID":    {Name: "OriginAirportID", Type: "keyword"},
		"OriginCityName":     {Name: "OriginCityName", Type: "keyword"},
		"OriginCountry":      {Name: "OriginCountry", Type: "keyword"},
		"OriginLocation":     {Name: "OriginLocation", Type: "point"},
		"OriginRegion":       {Name: "OriginRegion", Type: "keyword"},
		"OriginWeather":      {Name: "OriginWeather", Type: "keyword"},
		"dayOfWeek":          {Name: "dayOfWeek", Type: "long"},
		"timestamp":          {Name: "timestamp", Type: "timestamp"},
	}
	kibanaSampleEcommerceFields = map[string]schema.Column{
		"category":                      {Name: "category", Type: "text"},
		"currency":                      {Name: "currency", Type: "keyword"},
		"customer_birth_date":           {Name: "customer_birth_date", Type: "timestamp"},
		"customer_first_name":           {Name: "customer_first_name", Type: "text"},
		"customer_full_name":            {Name: "customer_full_name", Type: "text"},
		"customer_gender":               {Name: "customer_gender", Type: "keyword"},
		"customer_id":                   {Name: "customer_id", Type: "keyword"},
		"customer_last_name":            {Name: "customer_last_name", Type: "text"},
		"customer_phone":                {Name: "customer_phone", Type: "keyword"},
		"day_of_week":                   {Name: "day_of_week", Type: "keyword"},
		"day_of_week_i":                 {Name: "day_of_week_i", Type: "long"},
		"email":                         {Name: "email", Type: "keyword"},
		"event.dataset":                 {Name: "event.dataset", Type: "keyword"},
		"geoip.city_name":               {Name: "geoip.city_name", Type: "keyword"},
		"geoip.continent_name":          {Name: "geoip.continent_name", Type: "keyword"},
		"geoip.country_iso_code":        {Name: "geoip.country_iso_code", Type: "keyword"},
		"geoip.location":                {Name: "geoip.location", Type: "point"},
		"geoip.region_name":             {Name: "geoip.region_name", Type: "keyword"},
		"manufacturer":                  {Name: "manufacturer", Type: "text"},
		"order_date":                    {Name: "order_date", Type: "timestamp"},
		"order_id":                      {Name: "order_id", Type: "keyword"},
		"products._id":                  {Name: "products._id", Type: "text"},
		"products.base_price":           {Name: "products.base_price", Type: "float"},
		"products.base_unit_price":      {Name: "products.base_unit_price", Type: "float"},
		"products.category":             {Name: "products.category", Type: "text"},
		"products.created_on":           {Name: "products.created_on", Type: "timestamp"},
		"products.discount_amount":      {Name: "products.discount_amount", Type: "float"},
		"products.discount_percentage":  {Name: "products.discount_percentage", Type: "float"},
		"products.manufacturer":         {Name: "products.manufacturer", Type: "text"},
		"products.min_price":            {Name: "products.min_price", Type: "float"},
		"products.price":                {Name: "products.price", Type: "float"},
		"products.product_id":           {Name: "products.product_id", Type: "long"},
		"products.product_name":         {Name: "products.product_name", Type: "text"},
		"products.quantity":             {Name: "products.quantity", Type: "long"},
		"products.sku":                  {Name: "products.sku", Type: "keyword"},
		"products.tax_amount":           {Name: "products.tax_amount", Type: "float"},
		"products.taxful_price":         {Name: "products.taxful_price", Type: "float"},
		"products.taxless_price":        {Name: "products.taxless_price", Type: "float"},
		"products.unit_discount_amount": {Name: "products.unit_discount_amount", Type: "float"},
		"sku":                           {Name: "sku", Type: "keyword"},
		"taxful_total_price":            {Name: "taxful_total_price", Type: "float"},
		"taxless_total_price":           {Name: "taxless_total_price", Type: "float"},
		"total_quantity":                {Name: "total_quantity", Type: "long"},
		"total_unique_products":         {Name: "total_unique_products", Type: "long"},
		"type":                          {Name: "type", Type: "keyword"},
		"user":                          {Name: "user", Type: "keyword"},
	}
)

func newSchemaFromColumns(fields map[string]schema.Column) schema.Schema {
	schemaFields := make(map[schema.FieldName]schema.Field)
	for name, column := range fields {
		parsedType, _ := schema.ParseQuesmaType(column.Type)
		schemaFields[schema.FieldName(name)] = schema.Field{
			PropertyName:         schema.FieldName(name),
			InternalPropertyName: schema.FieldName(strings.Replace(name, ".", "::", -1)),
			Type:                 parsedType,
		}
	}
	return schema.NewSchema(schemaFields, true, "")
}

func TestParseMappings_KibanaSampleFlights(t *testing.T) {
	json := `{"properties": {
		"AvgTicketPrice": {
			"type": "float"
		},
		"Cancelled": {
			"type": "boolean"
		},
		"Carrier": {
			"type": "keyword"
		},
		"Dest": {
			"type": "keyword"
		},
		"DestAirportID": {
			"type": "keyword"
		},
		"DestCityName": {
			"type": "keyword"
		},
		"DestCountry": {
			"type": "keyword"
		},
		"DestLocation": {
			"type": "geo_point"
		},
		"DestRegion": {
			"type": "keyword"
		},
		"DestWeather": {
			"type": "keyword"
		},
		"DistanceKilometers": {
			"type": "float"
		},
		"DistanceMiles": {
			"type": "float"
		},
		"FlightDelay": {
			"type": "boolean"
		},
		"FlightDelayMin": {
			"type": "integer"
		},
		"FlightDelayType": {
			"type": "keyword"
		},
		"FlightNum": {
			"type": "keyword"
		},
		"FlightTimeHour": {
			"type": "keyword"
		},
		"FlightTimeMin": {
			"type": "float"
		},
		"Origin": {
			"type": "keyword"
		},
		"OriginAirportID": {
			"type": "keyword"
		},
		"OriginCityName": {
			"type": "keyword"
		},
		"OriginCountry": {
			"type": "keyword"
		},
		"OriginLocation": {
			"type": "geo_point"
		},
		"OriginRegion": {
			"type": "keyword"
		},
		"OriginWeather": {
			"type": "keyword"
		},
		"dayOfWeek": {
			"type": "integer"
		},
		"timestamp": {
			"type": "date"
		}
	}}`
	parsedJson, _ := types.ParseJSON(json)
	mappings := ParseMappings("", parsedJson)

	if !reflect.DeepEqual(mappings, kibanaSampleFlightsFields) {
		t.Errorf("ParseMappings() got = %v, want %v", mappings, kibanaSampleFlightsFields)
	}
}

func TestGenerateMappings_KibanaSampleFlights(t *testing.T) {
	expectedJson := `{"properties": {
		"AvgTicketPrice": {
			"type": "double"
		},
		"Cancelled": {
			"type": "boolean"
		},
		"Carrier": {
			"type": "keyword"
		},
		"Dest": {
			"type": "keyword"
		},
		"DestAirportID": {
			"type": "keyword"
		},
		"DestCityName": {
			"type": "keyword"
		},
		"DestCountry": {
			"type": "keyword"
		},
		"DestLocation": {
			"type": "geo_point"
		},
		"DestRegion": {
			"type": "keyword"
		},
		"DestWeather": {
			"type": "keyword"
		},
		"DistanceKilometers": {
			"type": "double"
		},
		"DistanceMiles": {
			"type": "double"
		},
		"FlightDelay": {
			"type": "boolean"
		},
		"FlightDelayMin": {
			"type": "long"
		},
		"FlightDelayType": {
			"type": "keyword"
		},
		"FlightNum": {
			"type": "keyword"
		},
		"FlightTimeHour": {
			"type": "keyword"
		},
		"FlightTimeMin": {
			"type": "double"
		},
		"Origin": {
			"type": "keyword"
		},
		"OriginAirportID": {
			"type": "keyword"
		},
		"OriginCityName": {
			"type": "keyword"
		},
		"OriginCountry": {
			"type": "keyword"
		},
		"OriginLocation": {
			"type": "geo_point"
		},
		"OriginRegion": {
			"type": "keyword"
		},
		"OriginWeather": {
			"type": "keyword"
		},
		"dayOfWeek": {
			"type": "long"
		},
		"timestamp": {
			"type": "date"
		}
	}}`
	s := newSchemaFromColumns(kibanaSampleFlightsFields)
	mappings := GenerateMappings(schema.SchemaToHierarchicalSchema(&s))

	marshaled, err := json.Marshal(mappings)
	assert.Nil(t, err)
	require.JSONEq(t, expectedJson, string(marshaled))
}

func TestParseMappings_KibanaSampleEcommerce(t *testing.T) {
	json := `{"properties": {
		"category": {
			"fields": {
				"keyword": {
					"type": "keyword"
				}
			},
			"type": "text"
		},
		"currency": {
			"type": "keyword"
		},
		"customer_birth_date": {
			"type": "date"
		},
		"customer_first_name": {
			"fields": {
				"keyword": {
					"ignore_above": 256,
					"type": "keyword"
				}
			},
			"type": "text"
		},
		"customer_full_name": {
			"fields": {
				"keyword": {
					"ignore_above": 256,
					"type": "keyword"
				}
			},                
			"type": "text"
		},
		"customer_gender": {
			"type": "keyword"
		},
		"customer_id": {
			"type": "keyword"
		},
		"customer_last_name": {
			"fields": {
				"keyword": {
					"ignore_above": 256,
					"type": "keyword"
				}
			},
			"type": "text"
		},
		"customer_phone": {
			"type": "keyword"
		},
		"day_of_week": {
			"type": "keyword"
		},
		"day_of_week_i": {
			"type": "integer"
		},
		"email": {
			"type": "keyword"
		},
		"event": {
			"properties": {
				"dataset": {
					"type": "keyword"
				}
			}
		},
		"geoip": {
			"properties": {
				"city_name": {
					"type": "keyword"
				},
				"continent_name": {
					"type": "keyword"
				},
				"country_iso_code": {
					"type": "keyword"
				},
				"location": {
					"type": "geo_point"
				},
				"region_name": {
					"type": "keyword"
				}
			}
		},
		"manufacturer": {
			"fields": {
				"keyword": {
					"type": "keyword"
				}
			},
			"type": "text"
		},
		"order_date": {
			"type": "date"
		},
		"order_id": {
			"type": "keyword"
		},
		"products": {
			"properties": {
				"_id": {
					"fields": {
						"keyword": {
							"ignore_above": 256,
							"type": "keyword"
						}
					},
					"type": "text"
				},
				"base_price": {
					"type": "half_float"
				},
				"base_unit_price": {
					"type": "half_float"
				},
				"category": {
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					},
					"type": "text"
				},
				"created_on": {
					"type": "date"
				},
				"discount_amount": {
					"type": "half_float"
				},
				"discount_percentage": {
					"type": "half_float"
				},
				"manufacturer": {
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					},
					"type": "text"
				},
				"min_price": {
					"type": "half_float"
				},
				"price": {
					"type": "half_float"
				},
				"product_id": {
					"type": "long"
				},
				"product_name": {
					"analyzer": "english",
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					},
					"type": "text"
				},
				"quantity": {
					"type": "integer"
				},
				"sku": {
					"type": "keyword"
				},
				"tax_amount": {
					"type": "half_float"
				},
				"taxful_price": {
					"type": "half_float"
				},
				"taxless_price": {
					"type": "half_float"
				},
				"unit_discount_amount": {
					"type": "half_float"
				}
			}
		},
		"sku": {
			"type": "keyword"
		},
		"taxful_total_price": {
			"type": "half_float"
		},
		"taxless_total_price": {
			"type": "half_float"
		},
		"total_quantity": {
			"type": "integer"
		},
		"total_unique_products": {
			"type": "integer"
		},
		"type": {
			"type": "keyword"
		},
		"user": {
			"type": "keyword"
		}
	}}`
	parsedJson, _ := types.ParseJSON(json)
	mappings := ParseMappings("", parsedJson)

	if !reflect.DeepEqual(mappings, kibanaSampleEcommerceFields) {
		t.Errorf("ParseMappings() got = %v, want %v", mappings, kibanaSampleEcommerceFields)
	}
}

func TestGenerateMappings_KibanaSampleEcommerce(t *testing.T) {
	expectedJson := `{"properties": {
		"category": {
			"fields": {
				"keyword": {
					"type": "keyword"
				}
			},
			"type": "text"
		},
		"currency": {
			"type": "keyword"
		},
		"customer_birth_date": {
			"type": "date"
		},
		"customer_first_name": {
			"fields": {
				"keyword": {
					"type": "keyword"
				}
			},
			"type": "text"
		},
		"customer_full_name": {
			"fields": {
				"keyword": {
					"type": "keyword"
				}
			},                
			"type": "text"
		},
		"customer_gender": {
			"type": "keyword"
		},
		"customer_id": {
			"type": "keyword"
		},
		"customer_last_name": {
			"fields": {
				"keyword": {
					"type": "keyword"
				}
			},
			"type": "text"
		},
		"customer_phone": {
			"type": "keyword"
		},
		"day_of_week": {
			"type": "keyword"
		},
		"day_of_week_i": {
			"type": "long"
		},
		"email": {
			"type": "keyword"
		},
		"event": {
			"properties": {
				"dataset": {
					"type": "keyword"
				}
			}
		},
		"geoip": {
			"properties": {
				"city_name": {
					"type": "keyword"
				},
				"continent_name": {
					"type": "keyword"
				},
				"country_iso_code": {
					"type": "keyword"
				},
				"location": {
					"type": "geo_point"
				},
				"region_name": {
					"type": "keyword"
				}
			}
		},
		"manufacturer": {
			"fields": {
				"keyword": {
					"type": "keyword"
				}
			},
			"type": "text"
		},
		"order_date": {
			"type": "date"
		},
		"order_id": {
			"type": "keyword"
		},
		"products": {
			"properties": {
				"_id": {
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					},
					"type": "text"
				},
				"base_price": {
					"type": "double"
				},
				"base_unit_price": {
					"type": "double"
				},
				"category": {
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					},
					"type": "text"
				},
				"created_on": {
					"type": "date"
				},
				"discount_amount": {
					"type": "double"
				},
				"discount_percentage": {
					"type": "double"
				},
				"manufacturer": {
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					},
					"type": "text"
				},
				"min_price": {
					"type": "double"
				},
				"price": {
					"type": "double"
				},
				"product_id": {
					"type": "long"
				},
				"product_name": {
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					},
					"type": "text"
				},
				"quantity": {
					"type": "long"
				},
				"sku": {
					"type": "keyword"
				},
				"tax_amount": {
					"type": "double"
				},
				"taxful_price": {
					"type": "double"
				},
				"taxless_price": {
					"type": "double"
				},
				"unit_discount_amount": {
					"type": "double"
				}
			}
		},
		"sku": {
			"type": "keyword"
		},
		"taxful_total_price": {
			"type": "double"
		},
		"taxless_total_price": {
			"type": "double"
		},
		"total_quantity": {
			"type": "long"
		},
		"total_unique_products": {
			"type": "long"
		},
		"type": {
			"type": "keyword"
		},
		"user": {
			"type": "keyword"
		}
	}}`
	s := newSchemaFromColumns(kibanaSampleEcommerceFields)
	mappings := GenerateMappings(schema.SchemaToHierarchicalSchema(&s))

	marshaled, err := json.Marshal(mappings)
	assert.Nil(t, err)
	require.JSONEq(t, expectedJson, string(marshaled))
}
