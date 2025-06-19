// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bulk

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/ingest"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_unmarshalElasticResponse(t *testing.T) {
	tests := []struct {
		name                    string
		bulkResponseFromElastic string
	}{
		{
			name:                    "bulk response with no errors (1)",
			bulkResponseFromElastic: `{"errors":false,"took":12,"items":[{"create":{"_index":"testcase15","_id":"XLkV5JABtqi1BREg-Ldw","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":7,"_primary_term":1,"status":201}}]}`,
		},
		{
			name:                    "bulk response with no errors (2)",
			bulkResponseFromElastic: `{"errors":false,"took":68,"items":[{"create":{"_index":"testcase15","_id":"XrkW5JABtqi1BREgWbeP","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":9,"_primary_term":1,"status":201}}]}`,
		},
		{
			name:                    "bulk response with some error",
			bulkResponseFromElastic: `{"errors":true,"took":28,"items":[{"create":{"_index":"testcase15","_id":"X7kW5JABtqi1BREgc7eg","status":400,"error":{"type":"document_parsing_exception","reason":"[1:14] failed to parse field [newcolumn] of type [long] in document with id 'X7kW5JABtqi1BREgc7eg'. Preview of field's value: 'invalid'","caused_by":{"type":"illegal_argument_exception","reason":"For input string: \"invalid\""}}}}]}`,
		},
	}

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			bulkResponse := &BulkResponse{}
			if err := json.Unmarshal([]byte(tt.bulkResponseFromElastic), bulkResponse); err != nil {
				t.Errorf("error while unmarshaling elastic response: %v", err)
			}

			marshaled, err := json.Marshal(bulkResponse)
			if err != nil {
				t.Errorf("error while marshaling elastic response: %v", err)
			}

			require.JSONEq(t, tt.bulkResponseFromElastic, string(marshaled), "unmarshaled and marshaled response should be the same")
		})
	}
}

func Test_BulkForEach(t *testing.T) {
	input := `{"create":{"_index":"kibana_sample_data_flights", "_id": "1"}}
{"FlightNum":"9HY9SWR","DestCountry":"AU","OriginWeather":"Sunny","OriginCityName":"Frankfurt am Main" }
{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager_8.11.1"}}
{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager"}}
{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager_X"}}
{"create":{"_index":"kibana_sample_data_flights", "_id": "2"}}
{"FlightNum":"FOO","DestCountry":"BAR","OriginWeather":"BAZ","OriginCityName":"QUIX" }
`
	ndjson, err := types.ParseNDJSON(input)
	assert.NoError(t, err)

	err = ndjson.BulkForEach(func(entryNumber int, operationParsed types.BulkOperation, operation types.JSON, doc types.JSON) error {
		switch entryNumber {
		case 0:
			assert.Equal(t, "create", operationParsed.GetOperation())
			assert.Equal(t, "kibana_sample_data_flights", operationParsed.GetIndex())
			assert.Equal(t, "9HY9SWR", doc["FlightNum"])

		case 1:
			assert.Equal(t, "delete", operationParsed.GetOperation())
			assert.Equal(t, ".kibana_task_manager_8.11.1", operationParsed.GetIndex())

		case 2:
			assert.Equal(t, "delete", operationParsed.GetOperation())
			assert.Equal(t, ".kibana_task_manager", operationParsed.GetIndex())

		case 3:
			assert.Equal(t, "delete", operationParsed.GetOperation())
			assert.Equal(t, ".kibana_task_manager_X", operationParsed.GetIndex())

		case 4:
			assert.Equal(t, "create", operationParsed.GetOperation())
			assert.Equal(t, "kibana_sample_data_flights", operationParsed.GetIndex())
			assert.Equal(t, "FOO", doc["FlightNum"])

		default:
			t.Errorf("Unexpected entry number: %d", entryNumber)
		}

		return nil
	})
	assert.NoError(t, err)
}

func Test_BulkForEachDeleteOnly(t *testing.T) {
	input := `{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager_8.11.1"}}`
	ndjson, err := types.ParseNDJSON(input)
	assert.NoError(t, err)

	err = ndjson.BulkForEach(func(entryNumber int, operationParsed types.BulkOperation, operation types.JSON, doc types.JSON) error {
		switch entryNumber {
		case 0:
			assert.Equal(t, "delete", operationParsed.GetOperation())
			assert.Equal(t, ".kibana_task_manager_8.11.1", operationParsed.GetIndex())
		default:
			t.Errorf("Unexpected entry number: %d", entryNumber)
		}

		return nil
	})
	assert.NoError(t, err)
}

// This table resolver will only route `kibana_sample_data_ecommerce` to ClickHouse, rest will go to Elasticsearch
var testTableResolver = table_resolver.NewDummyTableResolver(config.IndicesConfigs{
	"kibana_sample_data_ecommerce": config.IndexConfiguration{},
}, false)

func TestSplitBulkSampleData(t *testing.T) {
	ctx := context.Background()
	defaultIndex := ""
	var sampleDataPayload = `{"create":{"_index":"kibana_sample_data_ecommerce"}}
{"category":["Men's Clothing","Men's Accessories"],"currency":"EUR","customer_first_name":"Phil","customer_full_name":"Phil Carpenter","customer_gender":"MALE","customer_id":50,"customer_last_name":"Carpenter","customer_phone":"","day_of_week":"Saturday","day_of_week_i":5,"email":"phil@carpenter-family.zzz","manufacturer":["Low Tide Media","Angeldale"],"order_date":"2025-02-01T18:44:38+00:00","order_id":582458,"products":[{"base_price":18.99,"discount_percentage":0,"quantity":1,"manufacturer":"Low Tide Media","tax_amount":0,"product_id":6903,"category":"Men's Clothing","sku":"ZO0457004570","taxless_price":18.99,"unit_discount_amount":0,"min_price":9.87,"_id":"sold_product_582458_6903","discount_amount":0,"created_on":"2016-12-24T18:44:38+00:00","product_name":"Sweatshirt - black","price":18.99,"taxful_price":18.99,"base_unit_price":18.99},{"base_price":16.99,"discount_percentage":0,"quantity":1,"manufacturer":"Angeldale","tax_amount":0,"product_id":14737,"category":"Men's Accessories","sku":"ZO0701307013","taxless_price":16.99,"unit_discount_amount":0,"min_price":8.33,"_id":"sold_product_582458_14737","discount_amount":0,"created_on":"2016-12-24T18:44:38+00:00","product_name":"Belt - marron","price":16.99,"taxful_price":16.99,"base_unit_price":16.99}],"sku":["ZO0457004570","ZO0701307013"],"taxful_total_price":35.98,"taxless_total_price":35.98,"total_quantity":2,"total_unique_products":2,"type":"order","user":"phil","geoip":{"country_iso_code":"GB","location":{"lon":-0.1,"lat":51.5},"continent_name":"Europe"},"event":{"dataset":"sample_ecommerce"}}
{"create":{"_index":"kibana_sample_data_ecommerce"}}
{"category":["Women's Clothing"],"currency":"EUR","customer_first_name":"Elyssa","customer_full_name":"Elyssa Franklin","customer_gender":"FEMALE","customer_id":27,"customer_last_name":"Franklin","customer_phone":"","day_of_week":"Wednesday","day_of_week_i":2,"email":"elyssa@franklin-family.zzz","manufacturer":["Pyramidustries","Oceanavigations"],"order_date":"2025-01-29T04:14:53+00:00","order_id":577560,"products":[{"base_price":20.99,"discount_percentage":0,"quantity":1,"manufacturer":"Pyramidustries","tax_amount":0,"product_id":23179,"category":"Women's Clothing","sku":"ZO0178501785","taxless_price":20.99,"unit_discount_amount":0,"min_price":9.66,"_id":"sold_product_577560_23179","discount_amount":0,"created_on":"2016-12-21T04:14:53+00:00","product_name":"Sweatshirt - berry","price":20.99,"taxful_price":20.99,"base_unit_price":20.99},{"base_price":41.99,"discount_percentage":0,"quantity":1,"manufacturer":"Oceanavigations","tax_amount":0,"product_id":24283,"category":"Women's Clothing","sku":"ZO0259602596","taxless_price":41.99,"unit_discount_amount":0,"min_price":19.32,"_id":"sold_product_577560_24283","discount_amount":0,"created_on":"2016-12-21T04:14:53+00:00","product_name":"Wrap skirt - black","price":41.99,"taxful_price":41.99,"base_unit_price":41.99}],"sku":["ZO0178501785","ZO0259602596"],"taxful_total_price":62.98,"taxless_total_price":62.98,"total_quantity":2,"total_unique_products":2,"type":"order","user":"elyssa","geoip":{"country_iso_code":"US","location":{"lon":-74,"lat":40.8},"region_name":"New York","continent_name":"North America","city_name":"New York"},"event":{"dataset":"sample_ecommerce"}}
{"create":{"_index":"kibana_sample_data_ecommerce"}}
{"category":["Men's Accessories","Men's Shoes"],"currency":"EUR","customer_first_name":"Sultan Al","customer_full_name":"Sultan Al Greene","customer_gender":"MALE","customer_id":19,"customer_last_name":"Greene","customer_phone":"","day_of_week":"Wednesday","day_of_week_i":2,"email":"sultan al@greene-family.zzz","manufacturer":["Angeldale","Low Tide Media"],"order_date":"2025-01-29T05:18:14+00:00","order_id":577613,"products":[{"base_price":16.99,"discount_percentage":0,"quantity":1,"manufacturer":"Angeldale","tax_amount":0,"product_id":24869,"category":"Men's Accessories","sku":"ZO0700607006","taxless_price":16.99,"unit_discount_amount":0,"min_price":7.99,"_id":"sold_product_577613_24869","discount_amount":0,"created_on":"2016-12-21T05:18:14+00:00","product_name":"Belt business - black","price":16.99,"taxful_price":16.99,"base_unit_price":16.99},{"base_price":32.99,"discount_percentage":0,"quantity":1,"manufacturer":"Low Tide Media","tax_amount":0,"product_id":1550,"category":"Men's Shoes","sku":"ZO0384303843","taxless_price":32.99,"unit_discount_amount":0,"min_price":14.85,"_id":"sold_product_577613_1550","discount_amount":0,"created_on":"2016-12-21T05:18:14+00:00","product_name":"Casual lace-ups - dark brown","price":32.99,"taxful_price":32.99,"base_unit_price":32.99}],"sku":["ZO0700607006","ZO0384303843"],"taxful_total_price":49.98,"taxless_total_price":49.98,"total_quantity":2,"total_unique_products":2,"type":"order","user":"sultan","geoip":{"country_iso_code":"AE","location":{"lon":54.4,"lat":24.5},"region_name":"Abu Dhabi","continent_name":"Asia","city_name":"Abu Dhabi"},"event":{"dataset":"sample_ecommerce"}}
{"create":{"_index":"kibana_sample_data_ecommerce"}}
{"category":["Men's Shoes","Men's Accessories"],"currency":"EUR","customer_first_name":"Robbie","customer_full_name":"Robbie Shaw","customer_gender":"MALE","customer_id":48,"customer_last_name":"Shaw","customer_phone":"","day_of_week":"Wednesday","day_of_week_i":2,"email":"robbie@shaw-family.zzz","manufacturer":["Angeldale","Elitelligence"],"order_date":"2025-01-29T08:03:50+00:00","order_id":577774,"products":[{"base_price":64.99,"discount_percentage":0,"quantity":1,"manufacturer":"Angeldale","tax_amount":0,"product_id":1707,"category":"Men's Shoes","sku":"ZO0693906939","taxless_price":64.99,"unit_discount_amount":0,"min_price":35.74,"_id":"sold_product_577774_1707","discount_amount":0,"created_on":"2016-12-21T08:03:50+00:00","product_name":"Boots - Thistle","price":64.99,"taxful_price":64.99,"base_unit_price":64.99},{"base_price":24.99,"discount_percentage":0,"quantity":1,"manufacturer":"Elitelligence","tax_amount":0,"product_id":13366,"category":"Men's Accessories","sku":"ZO0604206042","taxless_price":24.99,"unit_discount_amount":0,"min_price":12.74,"_id":"sold_product_577774_13366","discount_amount":0,"created_on":"2016-12-21T08:03:50+00:00","product_name":"Across body bag - brown","price":24.99,"taxful_price":24.99,"base_unit_price":24.99}],"sku":["ZO0693906939","ZO0604206042"],"taxful_total_price":89.98,"taxless_total_price":89.98,"total_quantity":2,"total_unique_products":2,"type":"order","user":"robbie","geoip":{"country_iso_code":"AE","location":{"lon":55.3,"lat":25.3},"region_name":"Dubai","continent_name":"Asia","city_name":"Dubai"},"event":{"dataset":"sample_ecommerce"}}
{"create":{"_index":"kibana_sample_data_ecommerce"}}
{"category":["Women's Clothing","Women's Shoes"],"currency":"EUR","customer_first_name":"Wilhemina St.","customer_full_name":"Wilhemina St. Roberson","customer_gender":"FEMALE","customer_id":17,"customer_last_name":"Roberson","customer_phone":"","day_of_week":"Wednesday","day_of_week_i":2,"email":"wilhemina st.@roberson-family.zzz","manufacturer":["Tigress Enterprises MAMA","Tigress Enterprises"],"order_date":"2025-01-22T22:56:38+00:00","order_id":569250,"products":[{"base_price":32.99,"discount_percentage":0,"quantity":1,"manufacturer":"Tigress Enterprises MAMA","tax_amount":0,"product_id":22975,"category":"Women's Clothing","sku":"ZO0228902289","taxless_price":32.99,"unit_discount_amount":0,"min_price":17.48,"_id":"sold_product_569250_22975","discount_amount":0,"created_on":"2016-12-14T22:56:38+00:00","product_name":"Jersey dress - Medium Sea Green","price":32.99,"taxful_price":32.99,"base_unit_price":32.99},{"base_price":28.99,"discount_percentage":0,"quantity":1,"manufacturer":"Tigress Enterprises","tax_amount":0,"product_id":16886,"category":"Women's Shoes","sku":"ZO0005400054","taxless_price":28.99,"unit_discount_amount":0,"min_price":14.78,"_id":"sold_product_569250_16886","discount_amount":0,"created_on":"2016-12-14T22:56:38+00:00","product_name":"Wedges - black","price":28.99,"taxful_price":28.99,"base_unit_price":28.99}],"sku":["ZO0228902289","ZO0005400054"],"taxful_total_price":61.98,"taxless_total_price":61.98,"total_quantity":2,"total_unique_products":2,"type":"order","user":"wilhemina","geoip":{"country_iso_code":"MC","location":{"lon":7.4,"lat":43.7},"continent_name":"Europe","city_name":"Monte Carlo"},"event":{"dataset":"sample_ecommerce"}}
`
	bulk, err := types.ExpectNDJSON(types.ParseRequestBody(sampleDataPayload))
	if err != nil {
		t.Errorf("error while parsing ndjson: %v", err)
	}
	maxBulkSize := len(bulk)

	// first returned value here is a result of side effects (writes to ClickHouse and Elasticsearch) so it is not tested here
	_, clickhouseBulkEntries, elasticRequestBody, elasticBulkEntries, err := SplitBulk(ctx, &defaultIndex, bulk, maxBulkSize, testTableResolver, &ingest.NoOpIndexNameRewriter{})

	assert.NoError(t, err)
	assert.Len(t, clickhouseBulkEntries["kibana_sample_data_ecommerce"], 5)
	assert.Empty(t, elasticRequestBody)
	assert.Len(t, elasticBulkEntries, 0)
}

func TestSplitBulkDelete(t *testing.T) {
	ctx := context.Background()
	defaultIndex := ""
	var deleteOnlyPayload = `{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager_8.11.1"}}`

	bulk, err := types.ExpectNDJSON(types.ParseRequestBody(deleteOnlyPayload))
	if err != nil {
		t.Errorf("error while parsing ndjson: %v", err)
	}
	maxBulkSize := len(bulk)

	// first returned value here is a result of side effects (writes to ClickHouse and Elasticsearch) so it is not tested here
	_, clickhouseBulkEntries, elasticRequestBody, elasticBulkEntries, err := SplitBulk(ctx, &defaultIndex, bulk, maxBulkSize, testTableResolver, &ingest.NoOpIndexNameRewriter{})

	assert.NoError(t, err)
	assert.Len(t, clickhouseBulkEntries, 0)
	assert.NotEmpty(t, elasticRequestBody)
	assert.Equal(t, deleteOnlyPayload+"\n\n", string(elasticRequestBody))
	assert.Len(t, elasticBulkEntries, 1)
}

func TestSplitBulkUpdatePayload(t *testing.T) {
	ctx := context.Background()
	defaultIndex := ""
	var updatePayload = `{"update":{"_id":"task:reports:monitor","_index":".kibana_task_manager_8.11.1","if_seq_no":976,"if_primary_term":3}}
{"doc":{"task":{"retryAt":null,"runAt":"2025-01-17T16:02:28.305Z","startedAt":null,"params":"{}","ownerId":null,"schedule":{"interval":"3s"},"taskType":"reports:monitor","traceparent":"00-fc93dce588045ad01e1832457edc2df5-3e354838203799ea-00","state":"{}","scheduledAt":"2025-01-20T12:56:57.891Z","attempts":23,"status":"idle"},"updated_at":"2025-01-20T12:56:58.042Z"}}
{"update":{"_id":"task:Fleet-Usage-Logger-Task","_index":".kibana_task_manager_8.11.1","if_seq_no":977,"if_primary_term":3}}
{"doc":{"task":{"retryAt":null,"runAt":"2025-01-17T16:02:29.188Z","startedAt":null,"params":"{}","ownerId":null,"schedule":{"interval":"15m"},"taskType":"Fleet-Usage-Logger","scope":["fleet"],"traceparent":"00-fc93dce588045ad01e1832457edc2df5-3e354838203799ea-00","state":"{}","scheduledAt":"2025-01-20T12:56:57.891Z","attempts":8,"status":"idle"},"updated_at":"2025-01-20T12:56:58.042Z"}}
{"update":{"_id":"task:Alerts-alerts_invalidate_api_keys","_index":".kibana_task_manager_8.11.1","if_seq_no":978,"if_primary_term":3}}
{"doc":{"task":{"retryAt":null,"runAt":"2025-01-17T16:02:28.280Z","startedAt":null,"params":"{}","ownerId":null,"stateVersion":1,"schedule":{"interval":"5m"},"taskType":"alerts_invalidate_api_keys","traceparent":"00-fc93dce588045ad01e1832457edc2df5-3e354838203799ea-00","state":"{\"runs\":0,\"total_invalidated\":0}","scheduledAt":"2025-01-20T12:56:57.891Z","attempts":19,"status":"idle"},"updated_at":"2025-01-20T12:56:58.042Z"}}
{"update":{"_id":"task:security:endpoint-diagnostics:1.0.0","_index":".kibana_task_manager_8.11.1","if_seq_no":979,"if_primary_term":3}}
{"doc":{"task":{"retryAt":null,"runAt":"2025-01-17T16:02:28.291Z","startedAt":null,"params":"{\"version\":\"1.0.0\"}","ownerId":null,"stateVersion":1,"schedule":{"interval":"5m"},"taskType":"security:endpoint-diagnostics","scope":["securitySolution"],"traceparent":"00-fc93dce588045ad01e1832457edc2df5-3e354838203799ea-00","state":"{\"runs\":0}","scheduledAt":"2025-01-20T12:56:57.891Z","attempts":19,"status":"idle"},"updated_at":"2025-01-20T12:56:58.042Z"}}`

	bulk, err := types.ExpectNDJSON(types.ParseRequestBody(updatePayload))
	if err != nil {
		t.Errorf("error while parsing ndjson: %v", err)
	}
	maxBulkSize := len(bulk)

	// first returned value here is a result of side effects (writes to ClickHouse and Elasticsearch) so it is not tested here
	_, clickhouseBulkEntries, elasticRequestBody, elasticBulkEntries, err := SplitBulk(ctx, &defaultIndex, bulk, maxBulkSize, testTableResolver, &ingest.NoOpIndexNameRewriter{})

	assert.NoError(t, err)
	assert.Len(t, clickhouseBulkEntries, 0)
	assert.NotEmpty(t, elasticRequestBody)
	assert.Len(t, elasticBulkEntries, 4)
}

func TestSplitBulkMixedPayload(t *testing.T) {
	ctx := context.Background()
	defaultIndex := ""
	var mixedPayload = `{"update":{"_id":"task:reports:monitor","_index":".kibana_task_manager_8.11.1","if_seq_no":976,"if_primary_term":3}}
{"doc":{"task":{"retryAt":null,"runAt":"2025-01-17T16:02:28.305Z","startedAt":null,"params":"{}","ownerId":null,"schedule":{"interval":"3s"},"taskType":"reports:monitor","traceparent":"00-fc93dce588045ad01e1832457edc2df5-3e354838203799ea-00","state":"{}","scheduledAt":"2025-01-20T12:56:57.891Z","attempts":23,"status":"idle"},"updated_at":"2025-01-20T12:56:58.042Z"}}
{"delete":{"_id":"asdf","_index":"1"}}
{"delete":{"_id":"fasdfbdfgd","_index":"2"}}
{"create":{"_index":"kibana_sample_data_ecommerce"}}
{"category":["Men's Shoes","Men's Accessories"],"currency":"EUR","customer_first_name":"Robbie","customer_full_name":"Robbie Shaw","customer_gender":"MALE","customer_id":48,"customer_last_name":"Shaw","customer_phone":"","day_of_week":"Wednesday","day_of_week_i":2,"email":"robbie@shaw-family.zzz","manufacturer":["Angeldale","Elitelligence"],"order_date":"2025-01-29T08:03:50+00:00","order_id":577774,"products":[{"base_price":64.99,"discount_percentage":0,"quantity":1,"manufacturer":"Angeldale","tax_amount":0,"product_id":1707,"category":"Men's Shoes","sku":"ZO0693906939","taxless_price":64.99,"unit_discount_amount":0,"min_price":35.74,"_id":"sold_product_577774_1707","discount_amount":0,"created_on":"2016-12-21T08:03:50+00:00","product_name":"Boots - Thistle","price":64.99,"taxful_price":64.99,"base_unit_price":64.99},{"base_price":24.99,"discount_percentage":0,"quantity":1,"manufacturer":"Elitelligence","tax_amount":0,"product_id":13366,"category":"Men's Accessories","sku":"ZO0604206042","taxless_price":24.99,"unit_discount_amount":0,"min_price":12.74,"_id":"sold_product_577774_13366","discount_amount":0,"created_on":"2016-12-21T08:03:50+00:00","product_name":"Across body bag - brown","price":24.99,"taxful_price":24.99,"base_unit_price":24.99}],"sku":["ZO0693906939","ZO0604206042"],"taxful_total_price":89.98,"taxless_total_price":89.98,"total_quantity":2,"total_unique_products":2,"type":"order","user":"robbie","geoip":{"country_iso_code":"AE","location":{"lon":55.3,"lat":25.3},"region_name":"Dubai","continent_name":"Asia","city_name":"Dubai"},"event":{"dataset":"sample_ecommerce"}}
{"create":{"_index":"kibana_sample_data_ecommerce"}}
{"category":["Women's Clothing","Women's Shoes"],"currency":"EUR","customer_first_name":"Wilhemina St.","customer_full_name":"Wilhemina St. Roberson","customer_gender":"FEMALE","customer_id":17,"customer_last_name":"Roberson","customer_phone":"","day_of_week":"Wednesday","day_of_week_i":2,"email":"wilhemina st.@roberson-family.zzz","manufacturer":["Tigress Enterprises MAMA","Tigress Enterprises"],"order_date":"2025-01-22T22:56:38+00:00","order_id":569250,"products":[{"base_price":32.99,"discount_percentage":0,"quantity":1,"manufacturer":"Tigress Enterprises MAMA","tax_amount":0,"product_id":22975,"category":"Women's Clothing","sku":"ZO0228902289","taxless_price":32.99,"unit_discount_amount":0,"min_price":17.48,"_id":"sold_product_569250_22975","discount_amount":0,"created_on":"2016-12-14T22:56:38+00:00","product_name":"Jersey dress - Medium Sea Green","price":32.99,"taxful_price":32.99,"base_unit_price":32.99},{"base_price":28.99,"discount_percentage":0,"quantity":1,"manufacturer":"Tigress Enterprises","tax_amount":0,"product_id":16886,"category":"Women's Shoes","sku":"ZO0005400054","taxless_price":28.99,"unit_discount_amount":0,"min_price":14.78,"_id":"sold_product_569250_16886","discount_amount":0,"created_on":"2016-12-14T22:56:38+00:00","product_name":"Wedges - black","price":28.99,"taxful_price":28.99,"base_unit_price":28.99}],"sku":["ZO0228902289","ZO0005400054"],"taxful_total_price":61.98,"taxless_total_price":61.98,"total_quantity":2,"total_unique_products":2,"type":"order","user":"wilhemina","geoip":{"country_iso_code":"MC","location":{"lon":7.4,"lat":43.7},"continent_name":"Europe","city_name":"Monte Carlo"},"event":{"dataset":"sample_ecommerce"}}
{"delete":{"_id":"tesesfsrt","_index":"3"}}
`

	bulk, err := types.ExpectNDJSON(types.ParseRequestBody(mixedPayload))
	if err != nil {
		t.Errorf("error while parsing ndjson: %v", err)
	}
	maxBulkSize := len(bulk)

	results, clickhouseBulkEntries, elasticRequestBody, elasticBulkEntries, err := SplitBulk(ctx, &defaultIndex, bulk, maxBulkSize, testTableResolver, &ingest.NoOpIndexNameRewriter{})

	assert.NoError(t, err)
	assert.Len(t, results, maxBulkSize)
	assert.Len(t, clickhouseBulkEntries["kibana_sample_data_ecommerce"], 2)
	assert.NotEmpty(t, elasticRequestBody)
	assert.Len(t, elasticBulkEntries, 4)
}
