// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "quesma/quesma/ui"

var UnsupportedQueriesTests = []UnsupportedQueryTestCase{
	// bucket:
	{ // [0]
		TestName:  "bucket aggregation: adjacency_matrix",
		QueryType: "adjacency_matrix",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs" : {
				"interactions" : {
					"adjacency_matrix" : {
						"filters" : {
							"grpA" : { "terms" : { "accounts" : ["hillary", "sidney"] }},
							"grpB" : { "terms" : { "accounts" : ["donald", "mitt"] }},
							"grpC" : { "terms" : { "accounts" : ["vladimir", "nigel"] }}
						}
					}
				}
			}
		}`,
	},
	{ // [1]
		TestName:  "bucket aggregation: categorize_text",
		QueryType: "categorize_text",
		QueryRequestJson: `
		{
			"aggs": {
				"categories": {
					"categorize_text": {
						"field": "message"
					}
				}
			}
		}`,
	},
	{ // [2]
		TestName:  "bucket aggregation: children",
		QueryType: "children",
		QueryRequestJson: `
		{
			"aggs": {
				"top-tags": {
					"terms": {
						"field": "tags.keyword",
						"size": 10
					},
					"aggs": {
						"to-answers": {
							"children": {
								"type" : "answer" 
							},
							"aggs": {
								"top-names": {
									"terms": {
										"field": "owner.display_name.keyword",
										"size": 10
									}
								}
							}
						}
					}
				}
			}
		}`,
	},
	{ // [3]
		TestName:  "bucket aggregation: diversified_sampler",
		QueryType: "diversified_sampler",
		QueryRequestJson: `
		{
			"query": {
				"query_string": {
					"query": "tags:elasticsearch"
				}
			},
			"aggs": {
				"my_unbiased_sample": {
					"diversified_sampler": {
						"shard_size": 200,
						"field": "author"
					},
					"aggs": {
						"keywords": {
							"significant_terms": {
								"field": "tags",
								"exclude": [ "elasticsearch" ]
							}
						}
					}
				}
			}
		}`,
	},
	{ // [4]
		TestName:  "bucket aggregation: frequent_item_sets",
		QueryType: "frequent_item_sets",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_agg": {
					"frequent_item_sets": {
						"minimum_set_size": 3,
						"fields": [
							{
								"field": "category.keyword"
							},
							{
								"field": "geoip.city_name",
								"exclude": "other"
							}
						],
						"size": 3
					}
				}
			}
		}`,
	},
	{ // [5]
		TestName:  "bucket aggregation: geo_distance",
		QueryType: "geo_distance",
		QueryRequestJson: `
		{
			"aggs": {
				"rings": {
					"geo_distance": {
						"field": "location",
						"origin": "POINT (4.894 52.3760)",
						"unit": "km", 
						"ranges": [
							{ "to": 100 },
							{ "from": 100, "to": 300 },
							{ "from": 300 }
						]
					}
				}
			}
		}`,
	},
	{ // [6]
		TestName:  "bucket aggregation: geohash_grid",
		QueryType: "geohash_grid",
		QueryRequestJson: `
		{
			"aggs": {
				"zoomed-in": {
					"aggs": {
						"zoom1": {
							"geohash_grid": {
								"field": "location",
								"precision": 8
							}
						}
					}
				}
			}
		}`,
	},
	{ // [7]
		TestName:  "bucket aggregation: geohex_grid",
		QueryType: "geohex_grid",
		QueryRequestJson: `
		{
			"aggs": {
				"zoomed-in": {
					"aggs": {
						"zoom1": {
							"geohex_grid": {
								"field": "location",
								"precision": 12
							}
						}
					}
				}
			}
		}`,
	},
	{ // [8]
		TestName:  "bucket aggregation: global",
		QueryType: "global",
		QueryRequestJson: `
		{
			"query": {
				"match": { "type": "t-shirt" }
			},
			"aggs": {
				"all_products": {
					"global": {}, 
					"aggs": {     
						"avg_price": { "avg": { "field": "price" } }
					}
				},
				"t_shirts": { "avg": { "field": "price" } }
			}
		}`,
	},
	{ // [9]
		TestName:  "bucket aggregation: missing",
		QueryType: "missing",
		QueryRequestJson: `
		{
			"aggs": {
				"products_without_a_price": {
					"missing": { "field": "price" }
				}
			}
		}`,
	},
	{ // [10]
		TestName:  "bucket aggregation: nested",
		QueryType: "nested",
		QueryRequestJson: `
		{
			"query": {
				"match": {
					"name": "led tv"
				}
			},
			"aggs": {
				"resellers": {
					"nested": {
						"path": "resellers"
					},
					"aggs": {
						"min_price": {
							"min": {
								"field": "resellers.price"
							}
						}
					}
				}
			}
		}`,
	},
	{ // [11]
		TestName:  "bucket aggregation: parent",
		QueryType: "parent",
		QueryRequestJson: `
		{
			"aggs": {
				"top-names": {
					"terms": {
						"field": "owner.display_name.keyword",
						"size": 10
					},
					"aggs": {
						"to-questions": {
							"parent": {
								"type" : "answer" 
							},
							"aggs": {
								"top-tags": {
									"terms": {
										"field": "tags.keyword",
										"size": 10
									}
								}
							}
						}
					}
				}
			}
		}`,
	},
	{ // [12]
		TestName:  "bucket aggregation: rare_terms",
		QueryType: "rare_terms",
		QueryRequestJson: `
		{
			"aggs": {
				"genres": {
					"rare_terms": {
						"field": "genre"
					}
				}
			}
		}`,
	},
	{ // [13]
		TestName:  "bucket aggregation: reverse_nested",
		QueryType: "reverse_nested",
		QueryRequestJson: `
		{
			"query": {
				"match_all": {}
			},
			"aggs": {
				"comments": {
					"aggs": {
						"top_usernames": {
							"terms": {
								"field": "comments.username"
							},
							"aggs": {
								"comment_to_issue": {
									"reverse_nested": {}, 
									"aggs": {
										"top_tags_per_comment": {
											"terms": {
												"field": "tags"
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}`,
	},
	{ // [14]
		TestName:  "bucket aggregation: significant_text",
		QueryType: "significant_text",
		QueryRequestJson: `
		{
			"query": {
				"match": { "content": "Bird flu" }
			},
			"aggs": {
				"my_sample": {
					"sampler": {
						"shard_size": 100
					},
					"aggs": {
						"keywords": {
							"significant_text": { "field": "content" }
						}
					}
				}
			}
		}`,
	},
	{ // [15]
		TestName:  "bucket aggregation: time_series",
		QueryType: "time_series",
		QueryRequestJson: `
		{
			"aggs": {
				"ts": {
					"time_series": { "keyed": false }
				}
			}
		}`,
	},
	{ // [16]
		TestName:  "bucket aggregation: variable_width_histogram",
		QueryType: "variable_width_histogram",
		QueryRequestJson: `
		{
			"aggs": {
				"prices": {
					"variable_width_histogram": {
						"field": "price",
						"buckets": 2
					}
				}
			}
		}`,
	},
	// metrics:
	{ // [17]
		TestName:  "metrics aggregation: boxplot",
		QueryType: "boxplot",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"load_time_boxplot": {
					"boxplot": {
						"field": "load_time" 
					}
				}
			}
		}`,
	},
	{ // [18]
		TestName:  "metrics aggregation: geo_bounds",
		QueryType: "geo_bounds",
		QueryRequestJson: `
		{
			"aggs": {
				"viewport": {
					"geo_bounds": {
						"field": "geometry"
					}
				}
			}
		}`,
	},
	{ // [19]
		TestName:  "metrics aggregation: geo_line",
		QueryType: "geo_line",
		QueryRequestJson: `
		{
			"aggs": {
				"line": {
					"geo_line": {
						"point": {"field": "my_location"},
						"sort":  {"field": "@timestamp"}
					}
				}
			}
		}`,
	},
	{ // [20]
		TestName:  "metrics aggregation: cartesian_bounds",
		QueryType: "cartesian_bounds",
		QueryRequestJson: `
		{
			"query": {
				"match": { "name": "musée" }
			},
			"aggs": {
				"viewport": {
					"cartesian_bounds": {
						"field": "location"    
					}
				}
			}
		}`,
	},
	{ // [21]
		TestName:  "metrics aggregation: cartesian_centroid",
		QueryType: "cartesian_centroid",
		QueryRequestJson: `
		{
			"aggs": {
				"centroid": {
					"cartesian_centroid": {
						"field": "location" 
					}
				}
			}
		}`,
	},
	{ // [22]
		TestName:  "metrics aggregation: matrix_stats",
		QueryType: "matrix_stats",
		QueryRequestJson: `
		{
			"aggs": {
				"statistics": {
					"matrix_stats": {
						"fields": [ "poverty", "income" ]
					}
				}
			}
		}`,
	},
	{ // [23]
		TestName:  "metrics aggregation: median_absolute_deviation",
		QueryType: "median_absolute_deviation",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"review_average": {
					"avg": {
						"field": "rating"
					}
				},
				"review_variability": {
					"median_absolute_deviation": {
						"field": "rating" 
					}
				}
			}
		}`,
	},
	{ // [25]
		TestName:  "metrics aggregation: scripted_metric",
		QueryType: "scripted_metric",
		QueryRequestJson: `
		{
			"query": {
				"match_all": {}
			},
			"aggs": {
				"profit": {
					"scripted_metric": {
						"init_script": "state.transactions = []", 
						"map_script": "state.transactions.add(doc.type.value == 'sale' ? doc.amount.value : -1 * doc.amount.value)",
						"combine_script": "double profit = 0; for (t in state.transactions) { profit += t } return profit",
						"reduce_script": "double profit = 0; for (a in states) { profit += a } return profit"
					}
				}
			}
		}`,
	},
	{ // [26]
		TestName:  "metrics aggregation: string_stats",
		QueryType: "string_stats",
		QueryRequestJson: `
		{
			"aggs": {
				"message_stats": { "string_stats": { "field": "message.keyword" } }
			}
		}`,
	},
	{ // [35]
		TestName:  "metrics aggregation: t_test",
		QueryType: "t_test",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"startup_time_ttest": {
					"t_test": {
						"a": { "field": "startup_time_before" },  
						"b": { "field": "startup_time_after" },   
						"type": "paired"                          
					}
				}
			}
		}`,
	},
	{ // [27]
		TestName:  "metrics aggregation: weighted_avg",
		QueryType: "weighted_avg",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"weighted_grade": {
					"weighted_avg": {
						"value": {
							"field": "grade"
						},
						"weight": {
							"field": "weight"
						}
					}
				}
			}
		}`,
	},

	// pipeline:
	{ // [38]
		TestName:  "pipeline aggregation: bucket_count_ks_test",
		QueryType: "bucket_count_ks_test",
		QueryRequestJson: `
		{
			"aggs": {
				"buckets": {
					"terms": { 
						"field": "version",
						"size": 2
					},
					"aggs": {
						"latency_ranges": {
							"range": { 
								"field": "latency",
								"ranges": [
									{ "to": 0 },
									{ "from": 1775 }
								]
							}
						},
						"ks_test": { 
							"bucket_count_ks_test": {
								"buckets_path": "latency_ranges>_count",
								"alternative": ["less", "greater", "two_sided"]
							}
						}
					}
				}
			}
		}`,
	},
	{ // [39]
		TestName:  "pipeline aggregation: bucket_correlation",
		QueryType: "bucket_correlation",
		QueryRequestJson: `
		{
			"aggs": {
				"buckets": {
					"terms": { 
						"field": "version",
						"size": 2
					},
					"aggs": {
						"latency_ranges": {
							"range": { 
								"field": "latency",
								"ranges": [
									{ "to": 0.0 },
									{ "from": 0, "to": 105 },
									{ "from": 1555, "to": 1775 },
									{ "from": 1775 }
								]
							}
						},
						"bucket_correlation": { 
							"bucket_correlation": {
								"buckets_path": "latency_ranges>_count",
								"function": {
									"count_correlation": {
										"indicator": {
											"expectations": [0, 52.5, 165, 335, 555, 775, 1000, 1225, 1445, 1665, 1775],
											"doc_count": 200
										}
									}
								}
							}
						}
					}
				}
			}
		}`,
	},
	{ // [40]
		TestName:  "pipeline aggregation: bucket_selector",
		QueryType: "bucket_selector",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"sales_per_month": {
					"date_histogram": {
						"field": "date",
						"calendar_interval": "month"
					},
					"aggs": {
						"total_sales": {
							"sum": {
								"field": "price"
							}
						},
						"sales_bucket_filter": {
							"bucket_selector": {
								"buckets_path": {
									"totalSales": "total_sales"
								},
								"script": "params.totalSales > 200"
							}
						}
					}
				}
			}
		}`,
	},
	{ // [41]
		TestName:  "pipeline aggregation: bucket_sort",
		QueryType: "bucket_sort",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"sales_per_month": {
					"date_histogram": {
						"field": "date",
						"calendar_interval": "month"
					},
					"aggs": {
						"total_sales": {
							"sum": {
								"field": "price"
							}
						},
						"sales_bucket_sort": {
							"bucket_sort": {
								"sort": [
				  					{ "total_sales": { "order": "desc" } } 
								],
								"size": 3                                
							}
						}
					}
				}
			}
		}`,
	},
	{ // [42]
		TestName:  "pipeline aggregation: change_point",
		QueryType: "change_point",
		QueryRequestJson: `
		{
			"aggs": {
				"date": { 
					"date_histogram": {
						"field": "@timestamp",
						"fixed_interval": "1d"
					},
					"aggs": {
						"avg": { 
							"avg": {
								"field": "bytes"
							}
						}
					}
				},
				"change_points_avg": { 
					"change_point": {
						"buckets_path": "date>avg" 
					}
				}
			}
		}`,
	},
	{ // [43]
		TestName:  "pipeline aggregation: cumulative_cardinality",
		QueryType: "cumulative_cardinality",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"users_per_day": {
					"date_histogram": {
						"field": "timestamp",
						"calendar_interval": "day"
					},
					"aggs": {
						"distinct_users": {
							"cardinality": {
								"field": "user_id"
							}
						},
						"total_new_users": {
							"cumulative_cardinality": {
								"buckets_path": "distinct_users" 
							}
						}
					}
				}
			}
		}`,
	},
	{ // [46]
		TestName:  "pipeline aggregation: extended_stats_bucket",
		QueryType: "extended_stats_bucket",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"sales_per_month": {
					"date_histogram": {
						"field": "date",
						"calendar_interval": "month"
					},
					"aggs": {
						"sales": {
							"sum": {
								"field": "price"
							}
						}
					}
				},
				"stats_monthly_sales": {
					"extended_stats_bucket": {
						"buckets_path": "sales_per_month>sales" 
					}
				}
			}
		}`,
	},
	{ // [47]
		TestName:  "pipeline aggregation: inference",
		QueryType: "inference",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"client_ip": {
					"aggs": { 
						"url_dc": {
							"cardinality": {
								"field": "url.keyword"
							}
						},
						"bytes_sum": {
							"sum": {
								"field": "bytes"
							}
						},
						"geo_src_dc": {
							"cardinality": {
								"field": "geo.src"
							}
						},
						"geo_dest_dc": {
							"cardinality": {
								"field": "geo.dest"
							}
						},
						"responses_total": {
							"value_count": {
								"field": "timestamp"
							}
						},
						"success": {
							"filter": {
								"term": {
									"response": "200"
								}
							}
						},
						"error404": {
							"filter": {
								"term": {
									"response": "404"
								}
							}
						},
						"error503": {
							"filter": {
								"term": {
									"response": "503"
								}
							}
						},
						"malicious_client_ip": { 
							"inference": {
								"model_id": "malicious_clients_model",
								"buckets_path": {
									"response_count": "responses_total",
									"url_dc": "url_dc",
									"bytes_sum": "bytes_sum",
									"geo_src_dc": "geo_src_dc",
									"geo_dest_dc": "geo_dest_dc",
									"success": "success._count",
									"error404": "error404._count",
									"error503": "error503._count"
								}
							}
						}
					}
				}
			}
		}`,
	},
	{ // [50]
		TestName:  "pipeline aggregation: moving_fn",
		QueryType: "moving_fn",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_date_histo": {                  
					"date_histogram": {
						"field": "date",
						"calendar_interval": "1M"
					},
					"aggs": {
						"the_sum": {
							"sum": { "field": "price" }   
						},
						"the_movfn": {
							"moving_fn": {
								"buckets_path": "the_sum",  
								"window": 10,
								"script": "MovingFunctions.unweightedAvg(values)"
							}
						}
					}
				}
			}
		}`,
	},
	{ // [51]
		TestName:  "pipeline aggregation: moving_percentiles",
		QueryType: "moving_percentiles",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_date_histo": {                          
					"date_histogram": {
						"field": "date",
						"calendar_interval": "1M"
					},
					"aggs": {
						"the_percentile": {                     
							"percentiles": {
								"field": "price",
								"percents": [ 1.0, 99.0 ]
							}
						},
						"the_movperc": {
							"moving_percentiles": {
								"buckets_path": "the_percentile",   
								"window": 10
							}
						}
					}
				}
			}
		}`,
	},
	{ // [52]
		TestName:  "pipeline aggregation: normalize",
		QueryType: "normalize",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"sales_per_month": {
					"date_histogram": {
						"field": "date",
						"calendar_interval": "month"
					},
					"aggs": {
						"sales": {
							"sum": {
								"field": "price"
							}
						},
						"percent_of_total_sales": {
							"normalize": {
								"buckets_path": "sales",          
								"method": "percent_of_sum",       
								"format": "00.00%"                
							}
						}
					}
				}
			}
		}`,
	},
	{ // [53]
		TestName:  "pipeline aggregation: percentiles_bucket",
		QueryType: "percentiles_bucket",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"sales_per_month": {
					"date_histogram": {
						"field": "date",
						"calendar_interval": "month"
					},
					"aggs": {
						"sales": {
							"sum": {
								"field": "price"
							}
						}
					}
				},
				"percentiles_monthly_sales": {
					"percentiles_bucket": {
						"buckets_path": "sales_per_month>sales", 
						"percents": [ 25.0, 50.0, 75.0 ]         
					}
				}
			}
		}`,
	},
	{ // [55]
		TestName:  "pipeline aggregation: stats_bucket",
		QueryType: "stats_bucket",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"sales_per_month": {
					"date_histogram": {
						"field": "date",
						"calendar_interval": "month"
					},
					"aggs": {
						"sales": {
							"sum": {
								"field": "price"
							}
						}
					}
				},
				"stats_monthly_sales": {
					"stats_bucket": {
						"buckets_path": "sales_per_month>sales" 
					}
				}
			}
		}`,
	},
	// random non-existing aggregation:
	{ // [57]
		TestName:  "non-existing aggregation: Augustus_Caesar",
		QueryType: ui.UnrecognizedQueryType,
		QueryRequestJson: `
		{
			"query": {
				"match": { "content": "Bird flu" }
			},
			"aggs": {
				"my_sample": {
					"sampler": {
						"shard_size": 100
					},
					"aggs": {
						"keywords": {
							"Augustus_Caesar": { "field": "content" }
						}
					}
				}
			}
		}`,
	},

	// Query DSL Tests:
	{ // [58]
		TestName:  "Compound query: boosting",
		QueryType: "boosting",
		QueryRequestJson: `
		{
			"query": {
				"boosting": {
					"positive": {
						"term": {
							"text": "apple"
						}
					},
					"negative": {
						"term": {
							"text": "pie tart fruit crumble tree"
						}
					},
					"negative_boost": 0.5
				}
			}
		}`,
	},
	{ // [60]
		TestName:  "Compound query: disjunction_max",
		QueryType: "dis_max",
		QueryRequestJson: `
		{
			"query": {
				"dis_max": {
					"queries": [
						{ "term": { "title": "Quick pets" } },
						{ "term": { "body": "Quick pets" } }
					],
					"tie_breaker": 0.7
				}
			}
		}`,
	},
	{ // [61]
		TestName:  "Compound query: function score",
		QueryType: "function_score",
		QueryRequestJson: `
		{
			"query": {
				"function_score": {
					"query": { "match_all": {} },
					"boost": "5",
					"random_score": {}, 
					"boost_mode": "multiply"
				}
			}
		}`,
	},
	{ // [62]
		TestName:  "Full text queries: intervals",
		QueryType: "intervals",
		QueryRequestJson: `
		{
			"query": {
				"intervals" : {
					"my_text" : {
						"all_of" : {
							"ordered" : true,
							"intervals" : [
								{
									"match" : {
										"query" : "my favorite food",
										"max_gaps" : 0,
										"ordered" : true
									}
								},
								{
									"any_of" : {
										"intervals" : [
					  						{ "match" : { "query" : "hot water" } },
					  						{ "match" : { "query" : "cold porridge" } }
										]
									}
								}
							]
						}
					}
				}
			}
		}`,
	},
	{ // [63]
		TestName:  "Full text queries: match_bool_prefix",
		QueryType: "match_bool_prefix",
		QueryRequestJson: `
		{
			"query": {
				"match_bool_prefix" : {
					"message" : "quick brown f"
				}
			}
		}`,
	},
	{ // [64]
		TestName:  "Full text queries: match_phrase_prefix",
		QueryType: "match_phrase_prefix",
		QueryRequestJson: `
		{
			"query": {
				"match_phrase_prefix": {
					"message": {
						"query": "quick brown f"
					}
				}
			}
		}`,
	},
	{ // [65]
		TestName:  "Full text queries: combined fields",
		QueryType: "combined_fields",
		QueryRequestJson: `
		{
			"query": {
				"combined_fields" : {
					"query":      "database systems",
					"fields":     [ "title", "abstract", "body"],
					"operator":   "and"
				}
			}
		}`,
	},
	{ // [67]
		TestName:  "Geo queries: Geo-grid",
		QueryType: "geo_grid",
		QueryRequestJson: `
		{
			"query": {
				"geo_grid" :{
					"location" : {
						"geohash" : "u0"
					}
				}
			}
		}`,
	},
	{ // [68]
		TestName:  "Geo queries: Geo-polygon",
		QueryType: "geo_polygon",
		QueryRequestJson: `
		{
			"query": {
				"bool": {
					"must": {
						"match_all": {}
					},
					"filter": {
						"geo_polygon": {
							"person.location": {
								"points": [
									{ "lat": 40, "lon": -70 },
									{ "lat": 30, "lon": -80 },
									{ "lat": 20, "lon": -90 }
								]
							}
						}
					}
				}
			}
		}`,
	},
	{ // [69]
		TestName:  "Geo queries: geoshape",
		QueryType: "geo_shape",
		QueryRequestJson: `
		{
			"query": {
				"bool": {
					"must": {
						"match_all": {}
					},
					"filter": {
						"geo_shape": {
							"location": {
								"shape": {
									"type": "envelope",
									"coordinates": [ [ 13.0, 53.0 ], [ 14.0, 52.0 ] ]
								},
								"relation": "within"
							}
						}
					}
				}
			}
		}`,
	},
	{ // [70]
		TestName:  "Shape",
		QueryType: "shape",
		QueryRequestJson: `
		{
			"query": {
				"shape": {
					"geometry": {
						"shape": {
							"type": "envelope",
							"coordinates": [ [ 1355.0, 5355.0 ], [ 1400.0, 5200.0 ] ]
						},
						"relation": "within"
					}
				}
			}
		}`,
	},
	{ // [71]
		TestName:  "Joining queries: Has child",
		QueryType: "has_child",
		QueryRequestJson: `
		{
			"query": {
				"has_child": {
					"type": "child",
					"query": {
						"match_all": {}
					},
					"max_children": 10,
					"min_children": 2,
					"score_mode": "min"
				}
			}
		}`,
	},
	{ // [72]
		TestName:  "Joining queries: Has parent",
		QueryType: "has_parent",
		QueryRequestJson: `
		{
			"query": {
				"has_parent": {
					"parent_type": "parent",
					"query": {
						"term": {
							"tag": {
								"value": "Elasticsearch"
							}
						}
					}
				}
			}
		}`,
	},
	{ // [73]
		TestName:  "Joining queries: Parent id",
		QueryType: "parent_id",
		QueryRequestJson: `
		{
			"query": {
				"parent_id": {
					"type": "my-child",
					"id": "1"
				}
			}
		}`,
	},
	{ // [74]
		TestName:  "Span queries: Span containing",
		QueryType: "span_containing",
		QueryRequestJson: `
		{
			"query": {
				"span_containing": {
					"little": {
						"span_term": { "field1": "foo" }
					},
					"big": {
						"span_near": {
							"clauses": [
								{ "span_term": { "field1": "bar" } },
								{ "span_term": { "field1": "baz" } }
							],
							"slop": 5,
							"in_order": true
						}
					}
				}
			}
		}`,
	},
	{ // [75]
		TestName:  "Span queries: Span field masking",
		QueryType: "span_field_masking",
		QueryRequestJson: `
		{
			"query": {
				"span_field_masking": {
					"query": {
						"span_term": {
							"text.stems": "fox" 
						}
					},
					"field": "text" 
				}
			},
			"highlight": {
				"require_field_match" : false, 
				"fields": {
					"*": {}
				}
			}
		}`,
	},
	{ // [76]
		TestName:  "Span queries: Span first",
		QueryType: "span_first",
		QueryRequestJson: `
		{
			"query": {
				"span_first": {
					"match": {
						"span_term": { "user.id": "kimchy" }
					},
					"end": 3
				}
			}
		}`,
	},
	{ // [77]
		TestName:  "Span queries: Span multi-term",
		QueryType: "span_multi",
		QueryRequestJson: `
		{
			"query": {
				"span_multi": {
					"match": {
						"prefix": { "user.id": { "value": "ki" } }
					}
				}
			}
		}`,
	},
	{ // [78]
		TestName:  "Span queries: Span near",
		QueryType: "span_near",
		QueryRequestJson: `
		{
			"query": {
				"span_near": {
					"clauses": [
						{ "span_term": { "field": "value1" } },
						{ "span_term": { "field": "value2" } },
						{ "span_term": { "field": "value3" } }
					],
					"slop": 12,
					"in_order": false
				}
			}
		}`,
	},
	{ // [79]
		TestName:  "Span queries: Span not",
		QueryType: "span_not",
		QueryRequestJson: `
		{
			"query": {
				"span_not": {
					"include": {
						"span_term": { "field1": "hoya" }
					},
					"exclude": {
						"span_near": {
							"clauses": [
								{ "span_term": { "field1": "la" } },
								{ "span_term": { "field1": "hoya" } }
							],
							"slop": 0,
							"in_order": true
						}
					}
				}
			}
		}`,
	},
	{ // [80]
		TestName:  "Span queries: Span or",
		QueryType: "span_or",
		QueryRequestJson: `
		{
			"query": {
				"span_or" : {
					"clauses" : [
						{ "span_term" : { "field" : "value1" } },
						{ "span_term" : { "field" : "value2" } },
						{ "span_term" : { "field" : "value3" } }
					]
				}
			}
		}`,
	},
	{ // [81]
		TestName:  "Span queries: Span term",
		QueryType: "span_term",
		QueryRequestJson: `
		{
			"query": {
				"span_term" : { "user.id" : "kimchy" }
			}
		}`,
	},
	{ // [82]
		TestName:  "Span queries: Span within",
		QueryType: "span_within",
		QueryRequestJson: `
		{
			"query": {
				"span_within": {
					"little": {
						"span_term": { "field1": "foo" }
					},
					"big": {
						"span_near": {
							"clauses": [
								{ "span_term": { "field1": "bar" } },
								{ "span_term": { "field1": "baz" } }
							],
							"slop": 5,
							"in_order": true
						}
					}
				}
			}
		}`,
	},
	{ // [83]
		TestName:  "Specialized queries: Distance feature",
		QueryType: "distance_feature",
		QueryRequestJson: `
		{
			"query": {
				"bool": {
					"must": {
						"match": {
							"name": "chocolate"
						}
					},
					"should": {
						"distance_feature": {
							"field": "production_date",
							"pivot": "7d",
							"origin": "now"
						}
					},
					"minimum_should_match": 1
				}
			}
		}`,
	},
	{ // [84]
		TestName:  "Specialized queries: More like this",
		QueryType: "more_like_this",
		QueryRequestJson: `
		{
			"query": {
				"more_like_this": {
					"fields": [ "title", "description" ],
					"like": [
						{
							"_index": "imdb",
							"_id": "1"
						},
						{
							"_index": "imdb",
							"_id": "2"
						},
						"and potentially some more text here as well"
					],
					"min_term_freq": 1,
					"max_query_terms": 12
				}
			}
		}`,
	},
	{ // [85]
		TestName:  "Specialized queries: Percolate",
		QueryType: "percolate",
		QueryRequestJson: `
		{
			"query": {
				"percolate": {
					"field": "query",
					"document": {
						"message": "A new bonsai tree in the office"
					}
				}
			}
		}`,
	},
	{ // [86]
		TestName:  "Specialized queries: Knn",
		QueryType: "knn",
		QueryRequestJson: `
		{
			"size" : 3,
			"query" : {
				"knn": {
					"field": "image-vector",
					"query_vector": [-5, 9, -12],
					"num_candidates": 10
				}
			}
		}`,
	},
	{ // [87]
		TestName:  "Specialized queries: Rank feature",
		QueryType: "rank_feature",
		QueryRequestJson: `
		{
			"query": {
				"bool": {
					"must": [
						{
							"match": {
								"content": "2016"
							}
						}
					],
					"should": [
						{
							"rank_feature": {
								"field": "pagerank"
							}
						}
					],
					"minimum_should_match": 1
				}
			}
		}`,
	},
	{ // [88]
		TestName:  "Specialized queries: Script",
		QueryType: "script",
		QueryRequestJson: `
		{
			"query": {
				"bool": {
					"filter": {
						"script": {
							"script": """
							double amount = doc['amount'].value;
							if (doc['type'].value == 'expense') {
							  amount *= -1;
							}
							return amount < 10;
							"""
						}
					}
				}
			}
		}`,
	},
	{ // [89]
		TestName:  "Specialized queries: Script score",
		QueryType: "script_score",
		QueryRequestJson: `
		{
			"query": {
				"script_score": {
					"query": {
						"match": { "message": "elasticsearch" }
					},
					"script": {
						"source": "doc['my-int'].value / 10 "
					}
				}
			}
		}`,
	},
	{ // [90]
		TestName:  "Specialized queries: Wrapper",
		QueryType: "wrapper",
		QueryRequestJson: `
		{
			"query": {
				"wrapper": {
					"query": "eyJ0ZXJtIiA6IHsgInVzZXIuaWQiIDogImtpbWNoeSIgfX0=" 
				}
			}
		}`,
	},
	{ // [91]
		TestName:  "Specialized queries: Pinned query",
		QueryType: "pinned",
		QueryRequestJson: `
		{
			"query": {
				"pinned": {
					"ids": [ "1", "4", "100" ],
					"organic": {
						"match": {
							"description": "iphone"
						}
					}
				}
			}
		}`,
	},
	{ // [92]
		TestName:  "Specialized queries: Rule",
		QueryType: "rule_query",
		QueryRequestJson: `
		{
			"query": {
				"rule_query": {
					"match_criteria": {
						"user_query": "pugs"
					},
					"ruleset_id": "my-ruleset",
					"organic": {
						"match": {
							"description": "puggles"
						}
					}
				}
			}
		}`,
	},
	{ // [93]
		TestName:  "Specialized queries: Weighted tokens",
		QueryType: "weighted_tokens",
		QueryRequestJson: `
		{
			"query": {
				"weighted_tokens": {
					"query_expansion_field": {
						"tokens": {"2161": 0.4679, "2621": 0.307, "2782": 0.1299, "2851": 0.1056, "3088": 0.3041, "3376": 0.1038, "3467": 0.4873, "3684": 0.8958, "4380": 0.334, "4542": 0.4636, "4633": 2.2805, "4785": 1.2628, "4860": 1.0655, "5133": 1.0709, "7139": 1.0016, "7224": 0.2486, "7387": 0.0985, "7394": 0.0542, "8915": 0.369, "9156": 2.8947, "10505": 0.2771, "11464": 0.3996, "13525": 0.0088, "14178": 0.8161, "16893": 0.1376, "17851": 1.5348, "19939": 0.6012},
						"pruning_config": {
							"tokens_freq_ratio_threshold": 5,
							"tokens_weight_threshold": 0.4,
							"only_score_pruned_tokens": false
						}
					}
				}
			}
		}`,
	},
	{ // [94]
		TestName:  "Term-level queries: Fuzzy",
		QueryType: "fuzzy",
		QueryRequestJson: `
		{
			"query": {
				"fuzzy": {
					"user.id": {
						"value": "ki"
					}
				}
			}
		}`,
	},
	//{ // [95]
	//	The query is partially supported, doesn't blow up,
	// 	but the response is not as expected due to the nature of the backend (ClickHouse).
	//	TestName:  "Term-level queries: IDs",
	//	QueryType: "ids",
	//	QueryRequestJson: `
	//	{
	//		"query": {
	//			"ids" : {
	//				"values" : ["1", "4", "100"]
	//			}
	//		}
	//	}`,
	//},
	{ // [97]
		TestName:  "Term-level queries: Terms set",
		QueryType: "terms_set",
		QueryRequestJson: `
		{
			"query": {
				"terms_set": {
					"programming_languages": {
						"terms": [ "c++", "java", "php" ],
						"minimum_should_match_field": "required_matches"
					}
				}
			}
		}`,
	},
}
