// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains integration tests for different ingest functionalities.
// This is a good place to add regression tests for ingest bugs.

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"maps"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"text/tabwriter"
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
	if err != nil {
		return err
	}
	a.Containers = containers
	return nil
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
	t.Run("test supported types", func(t *testing.T) { a.testSupportedTypesInVanillaSetup(ctx, t) })
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

// Struct to parse only the `fields` tree
type Hit struct {
	Fields map[string][]any `json:"fields"`
	Source map[string]any   `json:"_source"`
}

type HitsWrapper struct {
	Hits []Hit `json:"hits"`
}

type Response struct {
	Hits HitsWrapper `json:"hits"`
}

func ParseResponse(t *testing.T, body []byte) map[string]any {
	var response Response
	err := json.Unmarshal([]byte(body), &response)
	if err != nil {
		log.Fatalf("Error parsing JSON: %v", err)
	}

	// Extract and print the `fields` tree
	for _, hit := range response.Hits.Hits {
		return hit.Source
	}
	return nil
}

func (a *IngestTestcase) testSupportedTypesInVanillaSetup(ctx context.Context, t *testing.T) {

	// Struct to parse only the `fields` tree
	type Hit struct {
		Fields map[string][]string `json:"fields"`
	}

	type HitsWrapper struct {
		Hits []Hit `json:"hits"`
	}

	type Response struct {
		Hits HitsWrapper `json:"hits"`
	}

	types := []struct {
		name        string
		ingestValue string
		queryValue  map[string]any
		description string
		supported   bool
	}{
		{
			name:        "binary",
			ingestValue: `"U29tZSBiaW5hcnkgZGF0YQ=="`,
			description: "Binary value encoded as a Base64 string.",
			queryValue:  map[string]any{"field_binary": "U29tZSBiaW5hcnkgZGF0YQ=="},
			supported:   true,
		},
		{
			name:        "boolean",
			ingestValue: "true",
			queryValue:  map[string]any{"field_boolean": true},
			description: "Represents `true` and `false` values.",
			supported:   true,
		},
		{
			name:        "keyword",
			ingestValue: `"example_keyword"`,
			description: "Used for structured content like tags, keywords, or identifiers.",
			queryValue:  map[string]any{"field_keyword": "example_keyword"},
			supported:   true,
		},
		{
			name:        "constant_keyword",
			ingestValue: `"fixed_value"`,
			description: "A keyword field for a single constant value across all documents.",
			queryValue:  map[string]any{"field_constant_keyword": "fixed_value"},
			supported:   true,
		},
		{
			name:        "wildcard",
			ingestValue: `"example*wildcard"`,
			description: "Optimized for wildcard search patterns.",
			queryValue:  map[string]any{"field_wildcard": "example*wildcard"},
			supported:   true,
		},
		{
			name:        "long",
			ingestValue: "1234",
			description: "64-bit integer value.",
			queryValue:  map[string]any{"field_long": 1234.0},
			supported:   true,
		},
		{
			name:        "double",
			ingestValue: "3.14159",
			description: "Double-precision 64-bit IEEE 754 floating point.",
			queryValue:  map[string]any{"field_double": 3.14159},
			supported:   true,
		},
		{
			name:        "date",
			ingestValue: `"2024-12-19"`,
			description: "Date value in ISO 8601 format.",
			queryValue:  map[string]any{"field_date": "2024-12-19"},
			supported:   true,
		},
		{
			name:        "date_nanos",
			ingestValue: `"2024-12-19T13:21:53.123456789Z"`,
			description: "Date value with nanosecond precision.",
			queryValue:  map[string]any{"field_date_nanos": "2024-12-19 13:21:53.123 +0000 UTC"},
			supported:   true,
		},
		{
			name:        "object",
			ingestValue: `{"name": "John", "age": 30}`,
			description: "JSON object containing multiple fields.",
			queryValue:  map[string]any{"field_object.name": "John", "field_object.age": 30.0},
			supported:   true,
		},
		{
			name:        "flattened",
			ingestValue: `{"key1": "value1", "key2": "value2"}`,
			description: "Entire JSON object as a single field value.",
			queryValue:  map[string]any{"field_flattened.key1": "value1", "field_flattened.key2": "value2"},
			supported:   true,
		},
		{
			name:        "nested",
			ingestValue: `[{"first": "John", "last": "Smith"}, {"first": "Alice", "last": "White"}]`,
			description: "Array of JSON objects preserving the relationship between subfields.",
			queryValue:  map[string]any{"field_nested_first": []string{"John", "Alice"}, "field_nested_last": []string{"Smith", "White"}},
			supported:   true,
		},
		{
			name:        "ip",
			ingestValue: `"192.168.1.1"`,
			description: "IPv4 or IPv6 address.",
			queryValue:  map[string]any{"field_ip": "192.168.1.1"},
			supported:   true,
		},
		{
			name:        "version",
			ingestValue: `"1.2.3"`,
			description: "Software version following Semantic Versioning.",
			queryValue:  map[string]any{"field_version": "1.2.3"},
			supported:   true,
		},
		{
			name:        "text",
			ingestValue: `"This is a full-text field."`,
			description: "Analyzed, unstructured text for full-text search.",
			queryValue:  map[string]any{"field_text": "This is a full-text field."},
			supported:   true,
		},
		{
			name:        "annotated-text",
			ingestValue: `"This is <entity>annotated</entity> text."`,
			description: "Text containing special markup for identifying named entities.",
			queryValue:  map[string]any{"field_annotated_text": "This is <entity>annotated</entity> text."},
			supported:   true,
		},
		{
			name:        "completion",
			ingestValue: `"autocomplete suggestion"`,
			description: "Used for auto-complete suggestions.",
			queryValue:  map[string]any{"field_completion": "autocomplete suggestion"},
			supported:   true,
		},
		{
			name:        "search_as_you_type",
			ingestValue: `"search as you type"`,
			description: "Text-like type for as-you-type completion.",
			queryValue:  map[string]any{"field_search_as_you_type": "search as you type"},
			supported:   true,
		},
		{
			name:        "dense_vector",
			ingestValue: `[0.1, 0.2, 0.3]`,
			queryValue:  map[string]any{"field_dense_vector": []float64{0.1, 0.2, 0.3}},
			description: "Array of float values representing a dense vector.",
			supported:   true,
		},
		{
			name:        "geo_point",
			ingestValue: `{"lat": 52.2297, "lon": 21.0122}`,
			queryValue:  map[string]any{"field_geo_point.lat": 52.2297, "field_geo_point.lon": 21.0122},
			description: "Latitude and longitude point.",
			supported:   true,
		},
		{
			name:        "geo_shape",
			ingestValue: `{"type": "polygon", "coordinates": [[[21.0, 52.0], [21.1, 52.0], [21.1, 52.1], [21.0, 52.1], [21.0, 52.0]]]}`,
			description: "Complex shapes like polygons.",
			supported:   true,
		},
		{
			name:        "integer_range",
			ingestValue: `{"gte": 10, "lte": 20}`,
			description: "Range of 32-bit integer values.",
			supported:   true,
		},
		{
			name:        "float_range",
			ingestValue: `{"gte": 1.5, "lte": 10.0}`,
			description: "Range of 32-bit floating-point values.",
			supported:   true,
		},
		{
			name:        "long_range",
			ingestValue: `{"gte": 1000000000, "lte": 2000000000}`,
			description: "Range of 64-bit integer values.",
			supported:   true,
		},
		{
			name:        "double_range",
			ingestValue: `{"gte": 2.5, "lte": 20.5}`,
			description: "Range of 64-bit double-precision floating-point values.",
			supported:   true,
		},
		{
			name:        "date_range",
			ingestValue: `{"gte": "2024-01-01", "lte": "2024-12-31"}`,
			description: "Range of date values, specified in ISO 8601 format.",
			supported:   true,
		},
		{
			name:        "ip_range",
			ingestValue: `{"gte": "192.168.0.0", "lte": "192.168.0.255"}`,
			description: "Range of IPv4 or IPv6 addresses.",
			supported:   true,
		},
	}

	type result struct {
		name           string
		claimedSupport bool
		currentSupport bool
		putMapping     bool
		ingest         bool
		query          bool
		errors         []string
		dbType         string
	}

	var results []*result

	for _, typ := range types {
		t.Run(typ.name, func(t *testing.T) {
			fmt.Println("Testing type: ", typ.name)

			r := &result{
				name:           typ.name,
				claimedSupport: typ.supported,
			}

			addError := func(s string) {
				r.errors = append(r.errors, s)
			}

			checkIfStatusOK := func(op string, resp *http.Response) bool {
				if resp.StatusCode != http.StatusOK {
					addError(fmt.Sprintf("failed HTTP request %s got status %d", op, resp.StatusCode))
					return false
				}
				return true
			}

			results = append(results, r)

			indexName := "types_test_" + typ.name
			fieldName := "field_" + typ.name

			resp, _ := a.RequestToQuesma(ctx, t, "PUT", "/"+indexName, []byte(`
{
	"mappings": {
		"properties": {
			"`+fieldName+`": {
				"type": "`+typ.name+`"
			},
		}
	},
	"settings": {
		"index": {}
	}
}`))

			r.putMapping = checkIfStatusOK("PUT mapping", resp)

			resp, _ = a.RequestToQuesma(ctx, t, "POST", fmt.Sprintf("/%s/_doc", indexName), []byte(`
{
	"`+fieldName+`": `+typ.ingestValue+`
}`))
			r.ingest = checkIfStatusOK("POST document", resp)

			resp, bytes := a.RequestToQuesma(ctx, t, "GET", "/"+indexName+"/_search", []byte(`
{ "query": { "match_all": {} } }
`))
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			fmt.Println("BODY", string(bytes))

			r.query = true
			source := ParseResponse(t, bytes)
			if source == nil {
				r.query = false
				addError("failed to parse quesma response")
			} else {

				if typ.queryValue == nil {
					addError("no query value provided")
				}

				for k, v := range typ.queryValue {
					if value, ok := source[k]; !ok {
						fmt.Println("EXPECTED", typ.queryValue, "GOT", source)
						r.query = false
						addError(fmt.Sprintf("field %s not found in response", k, value))
						continue
					} else {
						if !reflect.DeepEqual(value, v) {
							r.query = false
							addError(fmt.Sprintf("field %s has unexpected value %v", k, value))
							fmt.Println("EXPECTED", typ.queryValue, "GOT", source)
						}
					}
				}
			}

			columns, err := a.FetchClickHouseColumns(ctx, "quesma_common_table")

			if err != nil {
				t.Fatalf("failed to fetch 'quesma_common_table' columns: %v", err)
			} else {
				if dbType, ok := columns[fieldName]; ok {
					r.dbType = dbType
				} else {
					r.dbType = "n/a"
					prefix := fieldName + "_"

					for k, _ := range columns {
						if strings.HasPrefix(k, prefix) {
							r.dbType = fmt.Sprintf("column %s ...", k)
							break
						}
					}
				}
			}

			r.currentSupport = len(r.errors) == 0

			if r.claimedSupport && !r.currentSupport {
				t.Errorf("Type %s should be supported but is not: %v", r.name, r.errors)
			}
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].name < results[j].name
	})

	fmt.Println("")
	// Create a new tabwriter
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Print table header
	fmt.Fprintf(w, "Name\tSupport\tCurrent Support\tPut Mapping\tIngest\tQuery\tStored as\t\n")
	fmt.Fprintf(w, "----\t-------\t---------------\t-----------\t------\t-----\t---------\t\n")

	// Print rows
	for _, res := range results {
		fmt.Fprintf(w, "%s\t%v\t%v\t%v\t%v\t%v\t%v\t\n",
			res.name, res.claimedSupport, res.currentSupport, res.putMapping, res.ingest, res.query, res.dbType)
	}

	// Flush the writer to output
	w.Flush()

	fmt.Println("")

	var failedTypes []string

	for _, r := range results {

		if r.claimedSupport && !r.currentSupport {
			failedTypes = append(failedTypes, r.name)
		}
		if len(r.errors) > 0 {
			fmt.Println("Type: ", r.name)
			fmt.Println("Errors: ", strings.Join(r.errors, ", "))
		}
	}
}
