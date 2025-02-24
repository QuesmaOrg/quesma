// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains integration tests for different ingest functionalities.
// This is a good place to add regression tests for ingest bugs.

package testcases

import (
	"context"
	"github.com/stretchr/testify/assert"
	"maps"
	"net/http"
	"testing"
)

type IngestTestcase struct {
	IntegrationTestcaseBase
}

func NewIngestTestcase() *IngestTestcase {
	return &IngestTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-ingest.yml.template",
		},
	}
}

func (a *IngestTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *IngestTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })
	t.Run("test kibana_sample_data_flights ingest to ClickHouse", func(t *testing.T) { a.testKibanaSampleFlightsIngestToClickHouse(ctx, t) })
	t.Run("test kibana_sample_data_flights ingest to ClickHouse (with PUT mapping)", func(t *testing.T) { a.testKibanaSampleFlightsIngestWithMappingToClickHouse(ctx, t) })
	t.Run("test kibana_sample_data_ecommerce ingest to ClickHouse", func(t *testing.T) { a.testKibanaSampleEcommerceIngestToClickHouse(ctx, t) })
	t.Run("test kibana_sample_data_ecommerce ingest to ClickHouse (with PUT mapping)", func(t *testing.T) { a.testKibanaSampleEcommerceIngestWithMappingToClickHouse(ctx, t) })
	t.Run("test ignored fields", func(t *testing.T) { a.testIgnoredFields(ctx, t) })
	t.Run("test nested fields", func(t *testing.T) { a.testNestedFields(ctx, t) })
	t.Run("test field encodings (mappings bug)", func(t *testing.T) { a.testFieldEncodingsMappingsBug(ctx, t) })
	t.Run("test incomplete types", func(t *testing.T) { a.testIncompleteTypes(ctx, t) })
	return nil
}

func (a *IngestTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

var (
	expectedColsKibanaSampleFlights = map[string]string{
		"@timestamp":          "DateTime64(3)",
		"attributes_metadata": "Map(String, String)",
		"attributes_values":   "Map(String, String)",
		"avgticketprice":      "Nullable(Float64)",
		"cancelled":           "Nullable(Bool)",
		"carrier":             "Nullable(String)",
		"dayofweek":           "Nullable(Int64)",
		"dest":                "Nullable(String)",
		"destairportid":       "Nullable(String)",
		"destcityname":        "Nullable(String)",
		"destcountry":         "Nullable(String)",
		"destlocation_lat":    "Nullable(String)",
		"destlocation_lon":    "Nullable(String)",
		"destregion":          "Nullable(String)",
		"destweather":         "Nullable(String)",
		"distancekilometers":  "Nullable(Float64)",
		"distancemiles":       "Nullable(Float64)",
		"flightdelay":         "Nullable(Bool)",
		"flightdelaymin":      "Nullable(Int64)",
		"flightdelaytype":     "Nullable(String)",
		"flightnum":           "Nullable(String)",
		"flighttimehour":      "Nullable(Float64)",
		"flighttimemin":       "Nullable(Float64)",
		"origin":              "Nullable(String)",
		"originairportid":     "Nullable(String)",
		"origincityname":      "Nullable(String)",
		"origincountry":       "Nullable(String)",
		"originlocation_lat":  "Nullable(String)",
		"originlocation_lon":  "Nullable(String)",
		"originregion":        "Nullable(String)",
		"originweather":       "Nullable(String)",
		"timestamp":           "DateTime64(3)",
	}
	sampleDocKibanaSampleFlights  = []byte(`{"FlightNum":"9HY9SWR","DestCountry":"AU","OriginWeather":"Sunny","OriginCityName":"Frankfurt am Main","AvgTicketPrice":841.2656419677076,"DistanceMiles":10247.856675613455,"FlightDelay":false,"DestWeather":"Rain","Dest":"Sydney Kingsford Smith International Airport","FlightDelayType":"No Delay","OriginCountry":"DE","dayOfWeek":0,"DistanceKilometers":16492.32665375846,"timestamp":"2024-11-11T00:00:00","DestLocation":{"lat":"-33.94609833","lon":"151.177002"},"DestAirportID":"SYD","Carrier":"Kibana Airlines","Cancelled":false,"FlightTimeMin":1030.7704158599038,"Origin":"Frankfurt am Main Airport","OriginLocation":{"lat":"50.033333","lon":"8.570556"},"DestRegion":"SE-BD","OriginAirportID":"FRA","OriginRegion":"DE-HE","DestCityName":"Sydney","FlightTimeHour":17.179506930998397,"FlightDelayMin":0}`)
	putMappingKibanaSampleFlights = []byte(`
{
    "mappings": {
        "properties": {
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
        }
    },
    "settings": {
        "index": {}
    }
}`)

	expectedColsKibanaSampleEcommerce = map[string]string{
		"@timestamp":                    "DateTime64(3)",
		"attributes_metadata":           "Map(String, String)",
		"attributes_values":             "Map(String, String)",
		"category":                      "Array(String)",
		"currency":                      "Nullable(String)",
		"customer_first_name":           "Nullable(String)",
		"customer_full_name":            "Nullable(String)",
		"customer_gender":               "Nullable(String)",
		"customer_id":                   "Nullable(Int64)",
		"customer_last_name":            "Nullable(String)",
		"customer_phone":                "Nullable(String)",
		"day_of_week":                   "Nullable(String)",
		"day_of_week_i":                 "Nullable(Int64)",
		"email":                         "Nullable(String)",
		"event_dataset":                 "Nullable(String)",
		"geoip_city_name":               "Nullable(String)",
		"geoip_continent_name":          "Nullable(String)",
		"geoip_country_iso_code":        "Nullable(String)",
		"geoip_location_lat":            "Nullable(String)",
		"geoip_location_lon":            "Nullable(String)",
		"geoip_region_name":             "Nullable(String)",
		"manufacturer":                  "Array(String)",
		"order_date":                    "DateTime64(3)",
		"order_id":                      "Nullable(Int64)",
		"products__id":                  "Array(String)",
		"products_base_price":           "Array(Float64)",
		"products_base_unit_price":      "Array(Float64)",
		"products_category":             "Array(String)",
		"products_created_on":           "Array(DateTime64(3))",
		"products_discount_amount":      "Array(Int64)",
		"products_discount_percentage":  "Array(Int64)",
		"products_manufacturer":         "Array(String)",
		"products_min_price":            "Array(Float64)",
		"products_price":                "Array(Float64)",
		"products_product_id":           "Array(Int64)",
		"products_product_name":         "Array(String)",
		"products_quantity":             "Array(Int64)",
		"products_sku":                  "Array(String)",
		"products_tax_amount":           "Array(Int64)",
		"products_taxful_price":         "Array(Float64)",
		"products_taxless_price":        "Array(Float64)",
		"products_unit_discount_amount": "Array(Int64)",
		"sku":                           "Array(String)",
		"taxful_total_price":            "Nullable(Float64)",
		"taxless_total_price":           "Nullable(Float64)",
		"total_quantity":                "Nullable(Int64)",
		"total_unique_products":         "Nullable(Int64)",
		"type":                          "Nullable(String)",
		"user":                          "Nullable(String)",
	}
	sampleDocKibanaSampleEcommerce  = []byte(`{"category":["Men's Shoes","Men's Accessories"],"currency":"EUR","customer_first_name":"Thad","customer_full_name":"Thad Thompson","customer_gender":"MALE","customer_id":30,"customer_last_name":"Thompson","customer_phone":"","day_of_week":"Monday","day_of_week_i":0,"email":"thad@thompson-family.zzz","manufacturer":["Angeldale","Low Tide Media"],"order_date":"2024-12-02T16:59:31+00:00","order_id":585108,"products":[{"base_price":59.99,"discount_percentage":0,"quantity":1,"manufacturer":"Angeldale","tax_amount":0,"product_id":20830,"category":"Men's Shoes","sku":"ZO0684306843","taxless_price":59.99,"unit_discount_amount":0,"min_price":27.01,"_id":"sold_product_585108_20830","discount_amount":0,"created_on":"2016-12-26T16:59:31+00:00","product_name":"Casual lace-ups - sand","price":59.99,"taxful_price":59.99,"base_unit_price":59.99},{"base_price":21.99,"discount_percentage":0,"quantity":1,"manufacturer":"Low Tide Media","tax_amount":0,"product_id":12628,"category":"Men's Accessories","sku":"ZO0464504645","taxless_price":21.99,"unit_discount_amount":0,"min_price":11.43,"_id":"sold_product_585108_12628","discount_amount":0,"created_on":"2016-12-26T16:59:31+00:00","product_name":"Laptop bag - black/brown","price":21.99,"taxful_price":21.99,"base_unit_price":21.99}],"sku":["ZO0684306843","ZO0464504645"],"taxful_total_price":81.98,"taxless_total_price":81.98,"total_quantity":2,"total_unique_products":2,"type":"order","user":"thad","geoip":{"country_iso_code":"US","location":{"lon":-74,"lat":40.8},"region_name":"New York","continent_name":"North America","city_name":"New York"},"event":{"dataset":"sample_ecommerce"}}`)
	putMappingKibanaSampleEcommerce = []byte(`
{
    "mappings": {
        "properties": {
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
        }
    },
    "settings": {
        "index": {}
    }
}`)
)

func (a *IngestTestcase) testKibanaSampleFlightsIngestToClickHouse(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/kibana_sample_data_flights/_doc", sampleDocKibanaSampleFlights)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cols, err := a.FetchClickHouseColumns(ctx, "kibana_sample_data_flights")
	assert.NoError(t, err, "error fetching clickhouse columns")
	assert.Equal(t, expectedColsKibanaSampleFlights, cols)
}

func (a *IngestTestcase) testKibanaSampleFlightsIngestWithMappingToClickHouse(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "PUT", "/kibana_sample_data_flights_with_mappings", putMappingKibanaSampleFlights)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, _ = a.RequestToQuesma(ctx, t, "POST", "/kibana_sample_data_flights_with_mappings/_doc", sampleDocKibanaSampleFlights)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cols, err := a.FetchClickHouseColumns(ctx, "kibana_sample_data_flights_with_mappings")
	assert.NoError(t, err, "error fetching clickhouse columns")

	expectedCols := maps.Clone(expectedColsKibanaSampleFlights)

	// Because of the mappings, some types have changed (compared to ingest with schema inferred solely from JSON)
	expectedCols["timestamp"] = "Nullable(DateTime64(3))"

	expectedCols["destlocation_lat"] = "Nullable(String)"
	expectedCols["destlocation_lon"] = "Nullable(String)"

	expectedCols["flighttimehour"] = "Nullable(String)"

	assert.Equal(t, expectedCols, cols)
}

func (a *IngestTestcase) testKibanaSampleEcommerceIngestToClickHouse(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/kibana_sample_data_ecommerce/_doc", sampleDocKibanaSampleEcommerce)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cols, err := a.FetchClickHouseColumns(ctx, "kibana_sample_data_ecommerce")
	assert.NoError(t, err, "error fetching clickhouse columns")
	assert.Equal(t, expectedColsKibanaSampleEcommerce, cols)
}

func (a *IngestTestcase) testKibanaSampleEcommerceIngestWithMappingToClickHouse(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "PUT", "/kibana_sample_data_ecommerce_with_mappings", putMappingKibanaSampleEcommerce)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, _ = a.RequestToQuesma(ctx, t, "POST", "/kibana_sample_data_ecommerce_with_mappings/_doc", sampleDocKibanaSampleEcommerce)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cols, err := a.FetchClickHouseColumns(ctx, "kibana_sample_data_ecommerce_with_mappings")
	assert.NoError(t, err, "error fetching clickhouse columns")

	expectedCols := maps.Clone(expectedColsKibanaSampleEcommerce)

	// Because of the mappings, some types have changed (compared to ingest with schema inferred solely from JSON)
	expectedCols["order_date"] = "Nullable(DateTime64(3))"
	expectedCols["customer_birth_date"] = "Nullable(DateTime64(3))"

	expectedCols["customer_id"] = "Nullable(String)"
	expectedCols["order_id"] = "Nullable(String)"

	assert.Equal(t, expectedCols, cols)
}

func (a *IngestTestcase) testIgnoredFields(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/ignored_test/_doc", []byte(`
{
	"a": 1,
	"b": "first",
	"ignored_field1": 5,
	"nested.ignored_field3": 7,
	"nested2": {
		"ignored_field5": 6
	}
}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, _ = a.RequestToQuesma(ctx, t, "POST", "/ignored_test/_doc", []byte(`
{
	"a": 2,
	"b": "second",
	"ignored_field2": 11,
	"nested.ignored_field4": 8,
	"nested2": {
		"ignored_field6": 3
	}
}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cols, err := a.FetchClickHouseColumns(ctx, "ignored_test")
	assert.NoError(t, err, "error fetching clickhouse columns")

	expectedCols := map[string]string{
		"@timestamp":          "DateTime64(3)",
		"attributes_metadata": "Map(String, String)",
		"attributes_values":   "Map(String, String)",
		"a":                   "Nullable(Int64)",
		"b":                   "Nullable(String)",
	}
	assert.Equal(t, expectedCols, cols)
}

func (it *IngestTestcase) testNestedFields(ctx context.Context, t *testing.T) {
	resp, _ := it.RequestToQuesma(ctx, t, "POST", "/nested_test/_doc", []byte(`
{
	"a": "alpha",
	"b": "beta", 
	"c": "charlie",
	"nested.d": "delta",
	"nested2": {
		"e": "echo"
	}
}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, _ = it.RequestToQuesma(ctx, t, "POST", "/nested_test/_doc", []byte(`
{
	"a": "foxtrot", 
	"b": "golf",
	"c": "hotel",
	"nested.d": "india",
	"nested.f": "juliet",
	"nested2": {
		"e": "kilo",
		"g": "lima"
	}
}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify the data
	values := make([]interface{}, 7)
	valuePtrs := make([]interface{}, 7)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	rows, err := it.ExecuteClickHouseQuery(ctx, "SELECT a, b, c, nested_d, nested_f, nested2_e, nested2_g FROM nested_test ORDER BY a")
	assert.NoError(t, err)
	defer rows.Close()

	// First row
	assert.True(t, rows.Next())
	err = rows.Scan(valuePtrs...)
	assert.NoError(t, err)
	assert.Equal(t, "alpha", *values[0].(*string))
	assert.Equal(t, "beta", *values[1].(*string))
	assert.Equal(t, "charlie", *values[2].(*string))
	assert.Equal(t, "delta", *values[3].(*string))
	assert.Empty(t, values[4])
	assert.Equal(t, "echo", *values[5].(*string))
	assert.Empty(t, values[6])

	// Second row
	assert.True(t, rows.Next())
	err = rows.Scan(valuePtrs...)
	assert.NoError(t, err)
	assert.Equal(t, "foxtrot", *values[0].(*string))
	assert.Equal(t, "golf", *values[1].(*string))
	assert.Equal(t, "hotel", *values[2].(*string))
	assert.Equal(t, "india", *values[3].(*string))
	assert.Equal(t, "juliet", *values[4].(*string))
	assert.Equal(t, "kilo", *values[5].(*string))
	assert.Equal(t, "lima", *values[6].(*string))

	assert.False(t, rows.Next())
}

// Reproducer for issue #1045
func (a *IngestTestcase) testFieldEncodingsMappingsBug(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "PUT", "/encodings_test", []byte(`
{
	"mappings": {
		"properties": {
			"Field1": {
				"type": "text"
			},
			"Field2": {
				"type": "text"
			}
		}
	},
	"settings": {
		"index": {}
	}
}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, _ = a.RequestToQuesma(ctx, t, "POST", "/encodings_test/_doc", []byte(`
{
	"Field1": "abc"
}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, _ = a.RequestToQuesma(ctx, t, "POST", "/encodings_test/_doc", []byte(`
{
	"Field2": "cde"
}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	comments, err := a.FetchClickHouseComments(ctx, "encodings_test")
	assert.NoError(t, err, "error fetching clickhouse comments")

	assert.Equal(t, "quesmaMetadataV1:fieldName=Field1", comments["field1"])
	assert.Equal(t, "quesmaMetadataV1:fieldName=Field2", comments["field2"])
}

// Test "incomplete" types (e.g. null, empty array, empty object) for which Quesma can't infer a ClickHouse type.
func (a *IngestTestcase) testIncompleteTypes(ctx context.Context, t *testing.T) {
	doc := []byte(`
{
    "field1": "abc",
    "field2": null,

    "field3": ["def", "ghi"],
    "field4": [],
    "field5": [[]],

    "field6": {"ijk": "klm"},
    "field7": {},
    "field8": {"cde": {}},

    "field9": [[{"nop": "qrs"}]],
    "field10": [[{}]]
}
`)
	resp, body := a.RequestToQuesma(ctx, t, "POST", "/incomplete_types_test/_doc", doc)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, string(body), "error")

	cols, err := a.FetchClickHouseColumns(ctx, "incomplete_types_test")
	assert.NoError(t, err, "error fetching clickhouse columns")

	expectedCols := map[string]string{
		"@timestamp":          "DateTime64(3)",
		"attributes_metadata": "Map(String, String)",
		"attributes_values":   "Map(String, String)",

		"field1": "Nullable(String)",
		// field2 is null; null can be a Nullable(String), Nullable(Int64), ...

		"field3": "Array(String)",
		// field4 is an empty array; an empty array can be a Array(String), Array(Int64), ...
		// field5 is an array of empty array; it could be an Array(Array(String)), Array(Array(Int64)), ...

		"field6_ijk": "Nullable(String)",
		// field7 is an empty object; it could be a Tuple(field1 String), Tuple(field1 Int64, field2 String), ...
		// field8 is an object with an empty object; it could be a Tuple(cde Tuple(subfield Int64)), Tuple(cde Tuple(subfield String)), ...

		"field9": "Array(Array(Tuple(nop Nullable(String))))",
		// field10 is an array of arrays of empty objects; it could be an Array(Array(Tuple(subfield Int64))), Array(Array(Tuple(subfield String))), ...
	}
	assert.Equal(t, expectedCols, cols)

	// Insert a similar document again (now that the table is already created)
	doc2 := []byte(`
{
    "field1": "QUESMA_DOC2_1",
    "field2": null,

    "field3": ["QUESMA_DOC2_2", "QUESMA_DOC2_3"],
    "field4": [],
    "field5": [[]],

    "field6": {"ijk": "QUESMA_DOC2_4"},
    "field7": {},
    "field8": {"cde": {}},

    "field9": [[{"nop": "QUESMA_DOC2_5"}]],
    "field10": [[{}]]
}
`)
	resp, body = a.RequestToQuesma(ctx, t, "POST", "/incomplete_types_test/_doc", doc2)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, string(body), "error")

	cols, err = a.FetchClickHouseColumns(ctx, "incomplete_types_test")
	assert.NoError(t, err, "error fetching clickhouse columns")
	assert.Equal(t, expectedCols, cols)

	// Insert a document with all fields with complete types, testing ALTER TABLE ADD COLUMN behavior.
	doc3 := []byte(`
{
    "field1": "QUESMA_DOC3_1",
    "field2": "QUESMA_DOC3_2",

    "field3": ["QUESMA_DOC3_3", "QUESMA_DOC3_4"],
    "field4": ["QUESMA_DOC3_5"],
    "field5": [["QUESMA_DOC3_6"]],

    "field6": {"ijk": "QUESMA_DOC3_7"},
    "field7": {"klm": "QUESMA_DOC3_8"},
    "field8": {"cde": {"efg":"QUESMA_DOC3_9"}},

    "field9": [[{"nop": "QUESMA_DOC3_10"}]],
    "field10": [[{"asd": "QUESMA_DOC3_11"}]]
}
`)
	resp, body = a.RequestToQuesma(ctx, t, "POST", "/incomplete_types_test/_doc", doc3)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, string(body), "error")

	expectedCols = map[string]string{
		"@timestamp":          "DateTime64(3)",
		"attributes_metadata": "Map(String, String)",
		"attributes_values":   "Map(String, String)",

		"field1": "Nullable(String)",
		"field2": "Nullable(String)",

		"field3": "Array(String)",
		"field4": "Array(String)",
		"field5": "Array(Array(String))",

		"field6_ijk":     "Nullable(String)",
		"field7_klm":     "Nullable(String)",
		"field8_cde_efg": "Nullable(String)",

		"field9":  "Array(Array(Tuple(nop Nullable(String))))",
		"field10": "Array(Array(Tuple(asd Nullable(String))))",
	}
	cols, err = a.FetchClickHouseColumns(ctx, "incomplete_types_test")
	assert.NoError(t, err, "error fetching clickhouse columns")
	assert.Equal(t, expectedCols, cols)

	// Verify that DOC2 and DOC3 were correctly inserted.
	rows, err := a.ExecuteClickHouseQuery(ctx, "SELECT toString(field1), toString(field2), toString(field3), toString(field4), toString(field5), toString(field6_ijk), toString(field7_klm), toString(field8_cde_efg), toString(field9), toString(field10) FROM incomplete_types_test WHERE field1 IN ('QUESMA_DOC2_1', 'QUESMA_DOC3_1') ORDER BY field1")
	assert.NoError(t, err)
	defer rows.Close()

	var results []struct {
		cols []interface{}
	}
	for rows.Next() {
		r := make([]interface{}, 10)
		valPtrs := make([]interface{}, 10)
		for i := range r {
			valPtrs[i] = &r[i]
		}
		err = rows.Scan(valPtrs...)
		assert.NoError(t, err)
		results = append(results, struct{ cols []interface{} }{cols: r})
	}
	assert.Equal(t, 2, len(results))

	assert.Contains(t, *results[0].cols[0].(*string), "QUESMA_DOC2_1")
	assert.Contains(t, results[0].cols[2].(string), "QUESMA_DOC2_2")
	assert.Contains(t, results[0].cols[2].(string), "QUESMA_DOC2_3")
	assert.Contains(t, *results[0].cols[5].(*string), "QUESMA_DOC2_4")
	assert.Contains(t, results[0].cols[8].(string), "QUESMA_DOC2_5")

	assert.Contains(t, *results[1].cols[0].(*string), "QUESMA_DOC3_1")
	assert.Contains(t, *results[1].cols[1].(*string), "QUESMA_DOC3_2")
	assert.Contains(t, results[1].cols[2].(string), "QUESMA_DOC3_3")
	assert.Contains(t, results[1].cols[2].(string), "QUESMA_DOC3_4")
	assert.Contains(t, results[1].cols[3].(string), "QUESMA_DOC3_5")
	assert.Contains(t, results[1].cols[4].(string), "QUESMA_DOC3_6")
	assert.Contains(t, *results[1].cols[5].(*string), "QUESMA_DOC3_7")
	assert.Contains(t, *results[1].cols[6].(*string), "QUESMA_DOC3_8")
	assert.Contains(t, *results[1].cols[7].(*string), "QUESMA_DOC3_9")
	assert.Contains(t, results[1].cols[8].(string), "QUESMA_DOC3_10")
	assert.Contains(t, results[1].cols[9].(string), "QUESMA_DOC3_11")
}
