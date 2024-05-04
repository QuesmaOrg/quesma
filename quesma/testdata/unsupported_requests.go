package testdata

import "mitmproxy/quesma/quesma/ui"

var UnsupportedAggregationsTests = []UnsupportedAggregationTestCase{
	// bucket:
	{ // [0]
		TestName:        "bucket aggregation: adjacency_matrix",
		AggregationName: "adjacency_matrix",
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
		TestName:        "bucket aggregation: auto_date_histogram",
		AggregationName: "auto_date_histogram",
		QueryRequestJson: `
		{
			"aggs": {
				"sales_over_time": {
					"auto_date_histogram": {
						"field": "date",
						"buckets": 10
					}
				}
			}
		}`,
	},
	{ // [2]
		TestName:        "bucket aggregation: categorize_text",
		AggregationName: "categorize_text",
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
	{ // [3]
		TestName:        "bucket aggregation: children",
		AggregationName: "children",
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
	{ // [4]
		TestName:        "bucket aggregation: composite",
		AggregationName: "composite",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_buckets": {
					"composite": {
						"sources": [
							{ "product": { "terms": { "field": "product" } } }
						]
					}
				}
			}
		}`,
	},
	{ // [5]
		TestName:        "bucket aggregation: diversified_sampler",
		AggregationName: "diversified_sampler",
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
	{ // [6]
		TestName:        "bucket aggregation: frequent_item_sets",
		AggregationName: "frequent_item_sets",
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
	{ // [7]
		TestName:        "bucket aggregation: geo_distance",
		AggregationName: "geo_distance",
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
	{ // [8]
		TestName:        "bucket aggregation: geohash_grid",
		AggregationName: "geohash_grid",
		QueryRequestJson: `
		{
			"aggs": {
				"zoomed-in": {
					"filter": {
						"geo_bounding_box": {
							"location": {
								"top_left": "POINT (4.9 52.4)",
								"bottom_right": "POINT (5.0 52.3)"
							}
						}
					},
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
	{ // [9]
		TestName:        "bucket aggregation: geohex_grid",
		AggregationName: "geohex_grid",
		QueryRequestJson: `
		{
			"aggs": {
				"zoomed-in": {
					"filter": {
						"geo_bounding_box": {
							"location": {
								"top_left": "POINT (4.9 52.4)",
								"bottom_right": "POINT (5.0 52.3)"
							}
						}
					},
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
	{ // [10]
		TestName:        "bucket aggregation: geotile_grid",
		AggregationName: "geotile_grid",
		QueryRequestJson: `
		{
			"aggs": {
				"large-grid": {
					"geotile_grid": {
						"field": "location",
						"precision": 8
					}
				}
			}
		}`,
	},
	{ // [11]
		TestName:        "bucket aggregation: global",
		AggregationName: "global",
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
	{ // [12]
		TestName:        "bucket aggregation: ip_prefix",
		AggregationName: "ip_prefix",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"ipv4-subnets": {
					"ip_prefix": {
						"field": "ipv4",
						"prefix_length": 24
					}
				}
			}
		}`,
	},
	{ // [13]
		TestName:        "bucket aggregation: ip_range",
		AggregationName: "ip_range",
		QueryRequestJson: `
		{
			"size": 10,
			"aggs": {
				"ip_ranges": {
					"ip_range": {
						"field": "ip",
						"ranges": [
							{ "to": "10.0.0.5" },
							{ "from": "10.0.0.5" }
						]
					}
				}
			}
		}`,
	},
	{ // [14]
		TestName:        "bucket aggregation: missing",
		AggregationName: "missing",
		QueryRequestJson: `
		{
			"aggs": {
				"products_without_a_price": {
					"missing": { "field": "price" }
				}
			}
		}`,
	},
	{ // [15]
		TestName:        "bucket aggregation: multi_terms",
		AggregationName: "multi_terms",
		QueryRequestJson: `
		{
			"aggs": {
				"genres_and_products": {
					"multi_terms": {
						"terms": [
							{ "field": "genre" },
							{ "field": "product" }
						]
					}
				}
			}
		}`,
	},
	{ // [16]
		TestName:        "bucket aggregation: nested",
		AggregationName: "nested",
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
	{ // [17]
		TestName:        "bucket aggregation: parent",
		AggregationName: "parent",
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
	{ // [18]
		TestName:        "bucket aggregation: rare_terms",
		AggregationName: "rare_terms",
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
	{ // [19]
		TestName:        "bucket aggregation: reverse_nested",
		AggregationName: "reverse_nested",
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
	{ // [20]
		TestName:        "bucket aggregation: significant_text",
		AggregationName: "significant_text",
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
	{ // [21]
		TestName:        "bucket aggregation: time_series",
		AggregationName: "time_series",
		QueryRequestJson: `
		{
			"aggs": {
				"ts": {
					"time_series": { "keyed": false }
				}
			}
		}`,
	},
	{ // [22]
		TestName:        "bucket aggregation: variable_width_histogram",
		AggregationName: "variable_width_histogram",
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
	{ // [23]
		TestName:        "metrics aggregation: boxplot",
		AggregationName: "boxplot",
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
	{ // [24]
		TestName:        "metrics aggregation: extended_stats",
		AggregationName: "extended_stats",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"grades_stats": { "extended_stats": { "field": "grade" } }
			}
		}`,
	},
	{ // [25]
		TestName:        "metrics aggregation: geo_bounds",
		AggregationName: "geo_bounds",
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
	{ // [26]
		TestName:        "metrics aggregation: geo_centroid",
		AggregationName: "geo_centroid",
		QueryRequestJson: `
		{
			"aggs": {
				"cities": {
					"terms": { "field": "city.keyword" },
					"aggs": {
						"centroid": {
							"geo_centroid": { "field": "location" }
						}
					}
				}
			}
		}`,
	},
	{ // [27]
		TestName:        "metrics aggregation: geo_line",
		AggregationName: "geo_line",
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
	{ // [28]
		TestName:        "metrics aggregation: cartesian_bounds",
		AggregationName: "cartesian_bounds",
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
	{ // [29]
		TestName:        "metrics aggregation: cartesian_centroid",
		AggregationName: "cartesian_centroid",
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
	{ // [30]
		TestName:        "metrics aggregation: matrix_stats",
		AggregationName: "matrix_stats",
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
	{ // [31]
		TestName:        "metrics aggregation: median_absolute_deviation",
		AggregationName: "median_absolute_deviation",
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
	{ // [32]
		TestName:        "metrics aggregation: rate",
		AggregationName: "rate",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"by_date": {
					"date_histogram": {
						"field": "date",
						"calendar_interval": "month"  
					},
					"aggs": {
						"my_rate": {
							"rate": {
								"unit": "year"  
							}
						}
					}
				}
			}
		}`,
	},
	{ // [33]
		TestName:        "metrics aggregation: scripted_metric",
		AggregationName: "scripted_metric",
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
	{ // [34]
		TestName:        "metrics aggregation: string_stats",
		AggregationName: "string_stats",
		QueryRequestJson: `
		{
			"aggs": {
				"message_stats": { "string_stats": { "field": "message.keyword" } }
			}
		}`,
	},
	{ // [35]
		TestName:        "metrics aggregation: t_test",
		AggregationName: "t_test",
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
	{ // [36]
		TestName:        "metrics aggregation: weighted_avg",
		AggregationName: "weighted_avg",
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
	{ // [37]
		TestName:        "pipeline aggregation: avg_bucket",
		AggregationName: "avg_bucket",
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
				"avg_monthly_sales": {               
					"avg_bucket": {
						"buckets_path": "sales_per_month>sales",
						"gap_policy": "skip",
						"format": "#,##0.00;(#,##0.00)"
					}               
				}
			}
		}`,
	},
	{ // [38]
		TestName:        "pipeline aggregation: bucket_count_ks_test",
		AggregationName: "bucket_count_ks_test",
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
		TestName:        "pipeline aggregation: bucket_correlation",
		AggregationName: "bucket_correlation",
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
		TestName:        "pipeline aggregation: bucket_selector",
		AggregationName: "bucket_selector",
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
		TestName:        "pipeline aggregation: bucket_sort",
		AggregationName: "bucket_sort",
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
		TestName:        "pipeline aggregation: change_point",
		AggregationName: "change_point",
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
		TestName:        "pipeline aggregation: cumulative_cardinality",
		AggregationName: "cumulative_cardinality",
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
	{ // [44]
		TestName:        "pipeline aggregation: cumulative_sum",
		AggregationName: "cumulative_sum",
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
						"cumulative_sales": {
							"cumulative_sum": {
								"buckets_path": "sales" 
							}
						}
					}
				}
			}
		}`,
	},
	{ // [45]
		TestName:        "pipeline aggregation: derivative",
		AggregationName: "derivative",
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
						"sales_deriv": {
							"derivative": {
								"buckets_path": "sales" 
							}
						}
					}
				}
			}
		}`,
	},
	{ // [46]
		TestName:        "pipeline aggregation: extended_stats_bucket",
		AggregationName: "extended_stats_bucket",
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
		TestName:        "pipeline aggregation: inference",
		AggregationName: "inference",
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
	{ // [48]
		TestName:        "pipeline aggregation: max_bucket",
		AggregationName: "max_bucket",
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
				"max_monthly_sales": {
					"max_bucket": {
						"buckets_path": "sales_per_month>sales" 
					}
				}
			}
		}`,
	},
	{ // [49]
		TestName:        "pipeline aggregation: min_bucket",
		AggregationName: "min_bucket",
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
				"min_monthly_sales": {
					"min_bucket": {
						"buckets_path": "sales_per_month>sales" 
					}
				}
			}
		}`,
	},
	{ // [50]
		TestName:        "pipeline aggregation: moving_fn",
		AggregationName: "moving_fn",
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
		TestName:        "pipeline aggregation: moving_percentiles",
		AggregationName: "moving_percentiles",
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
		TestName:        "pipeline aggregation: normalize",
		AggregationName: "normalize",
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
		TestName:        "pipeline aggregation: percentiles_bucket",
		AggregationName: "percentiles_bucket",
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
	{ // [54]
		TestName:        "pipeline aggregation: serial_diff",
		AggregationName: "serial_diff",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_date_histo": {                  
					"date_histogram": {
						"field": "timestamp",
						"calendar_interval": "day"
					},
					"aggs": {
						"the_sum": {
							"sum": {
								"field": "lemmings"     
							}
						},
						"thirtieth_difference": {
							"serial_diff": {                
								"buckets_path": "the_sum",
								"lag" : 30
							}
						}
					}
				}
			}
		}`,
	},
	{ // [55]
		TestName:        "pipeline aggregation: stats_bucket",
		AggregationName: "stats_bucket",
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
	{ // [56]
		TestName:        "pipeline aggregation: sum_bucket",
		AggregationName: "sum_bucket",
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
				"sum_monthly_sales": {
					"sum_bucket": {
						"buckets_path": "sales_per_month>sales" 
					}
				}
			}
		}`,
	},
	// random non-existing aggregation:
	{ // [57]
		TestName:        "non-existing aggregation: Augustus_Caesar",
		AggregationName: ui.UnrecognizedQueryType,
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
}
