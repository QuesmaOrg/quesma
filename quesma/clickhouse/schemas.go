package clickhouse

var NewRuntimeSchemas = make(TableMap)

// 28 tables now
// A solution for now to remember tables. Didn't want to bother with config files at POC stage.
// Generated via DumpTableSchemas() and later ShortenDumpSchemasOutput()
var PredefinedTableSchemas = TableMap{
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_15": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_15",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_18": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_18",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "stateVersion",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scope",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_3": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_3",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_7": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_7",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_12": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_12",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scope",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "stateVersion",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_13": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_13",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_14": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_14",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "stateVersion",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scope",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_17": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_17",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_2": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_2",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "stateVersion",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_5": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_5",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_10": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_10",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scope",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_9": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_9",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&require_alias=true_1": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&require_alias=true_1",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"delete": &Column{
				Name: "delete",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_16": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_16",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "stateVersion",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scope",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_19": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_19",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_6": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_6",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scope",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_8": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_8",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scope",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_3": &Table{
		Created:  false,
		Name:     "/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_3",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"index": &Column{
				Name: "index",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/.kibana-event-log-ds/_bulk_2": &Table{
		Created:  false,
		Name:     "/.kibana-event-log-ds/_bulk_2",
		Database: "",
		Cluster:  "",
		Cols:     map[string]*Column{},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_4": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_4",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "stateVersion",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_1": &Table{
		Created:  false,
		Name:     "/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_1",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"index": &Column{
				Name: "index",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_2": &Table{
		Created:  false,
		Name:     "/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_2",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"kibana": &Column{
				Name: "kibana",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "snapshot",
							Type: BaseType{
								Name:   "Bool",
								goType: NewBaseType("Bool").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "status",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "uuid",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "name",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "host",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "transport_address",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "version",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"process": &Column{
				Name: "process",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "Tuple",
										Type: MultiValueType{
											Name: "Tuple",
											Cols: []*Column{
												&Column{
													Name: "total_in_bytes",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
												&Column{
													Name: "used_in_bytes",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
												&Column{
													Name: "size_limit",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
											},
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "resident_set_size_in_bytes",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "event_loop_delay",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "fromTimestamp",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "lastUpdatedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: MultiValueType{
											Name: "Tuple",
											Cols: []*Column{
												&Column{
													Name: "50",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
												&Column{
													Name: "75",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
												&Column{
													Name: "95",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
												&Column{
													Name: "99",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
											},
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "min",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "max",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "mean",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "exceeds",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "stddev",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "active",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "idle",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "utilization",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "uptime_in_millis",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"processes": &Column{
				Name: "processes",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"requests": &Column{
				Name: "requests",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "disconnects",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "total",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"response_times": &Column{
				Name: "response_times",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "average",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "max",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"concurrent_connections": &Column{
				Name: "concurrent_connections",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"elasticsearch_client": &Column{
				Name: "elasticsearch_client",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "totalQueuedRequests",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "totalActiveSockets",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "totalIdleSockets",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"os": &Column{
				Name: "os",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "1m",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "5m",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "15m",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "total_in_bytes",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "free_in_bytes",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "used_in_bytes",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "distro",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "cfs_quota_micros",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "cfs_period_micros",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "control_group",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: MultiValueType{
											Name: "Tuple",
											Cols: []*Column{
												&Column{
													Name: "number_of_elapsed_periods",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
												&Column{
													Name: "number_of_times_throttled",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
												&Column{
													Name: "time_throttled_nanos",
													Type: BaseType{
														Name:   "String",
														goType: NewBaseType("String").goType,
													},
													Codec: Codec{
														Name: "",
													},
												},
											},
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "platform",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "platformRelease",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "control_group",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "usage_nanos",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "uptime_in_millis",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "distroRelease",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_4": &Table{
		Created:  false,
		Name:     "/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_4",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"kibana": &Column{
				Name: "kibana",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "transport_address",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "version",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "status",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "uuid",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "host",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "locale",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "port",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "name",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "snapshot",
							Type: BaseType{
								Name:   "Bool",
								goType: NewBaseType("Bool").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"xpack": &Column{
				Name: "xpack",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/.kibana-event-log-ds/_bulk_1": &Table{
		Created:  false,
		Name:     "/.kibana-event-log-ds/_bulk_1",
		Database: "",
		Cluster:  "",
		Cols:     map[string]*Column{},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_1": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_1",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_11": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_11",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"update": &Column{
				Name: "update",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "_id",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "_index",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_seq_no",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "if_primary_term",
							Type: BaseType{
								Name:   "String",
								goType: NewBaseType("String").goType,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/_bulk?refresh=false&_source_includes=originId&require_alias=true_20": &Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_20",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": &Column{
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						&Column{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									&Column{
										Name: "ownerId",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "stateVersion",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "status",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "retryAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "startedAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scope",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "attempts",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "runAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "params",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "taskType",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "scheduledAt",
										Type: BaseType{
											Name:   "DateTime64",
											goType: nil,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "Tuple",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "traceparent",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
									&Column{
										Name: "state",
										Type: BaseType{
											Name:   "String",
											goType: NewBaseType("String").goType,
										},
										Codec: Codec{
											Name: "",
										},
									},
								},
							},
							Codec: Codec{
								Name: "",
							},
						},
						&Column{
							Name: "updated_at",
							Type: BaseType{
								Name:   "DateTime64",
								goType: nil,
							},
							Codec: Codec{
								Name: "",
							},
						},
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
	"/logs-generic-default/_doc": &Table{
		Created:  false,
		Name:     "/logs-generic-default/_doc",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"host_name": &Column{
				Name: "host_name",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"message": &Column{
				Name: "message",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"service_name": &Column{
				Name: "service_name",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"attributes_int64_key": &Column{
				Name: "attributes_int64_key",
				Type: CompoundType{
					Name: "Array",
					BaseType: BaseType{
						Name:   "String",
						goType: NewBaseType("String").goType,
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"attributes_int64_value": &Column{
				Name: "attributes_int64_value",
				Type: CompoundType{
					Name: "Array",
					BaseType: BaseType{
						Name:   "Int64",
						goType: NewBaseType("Int64").goType,
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"severity": &Column{
				Name: "severity",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"attributes_bool_value": &Column{
				Name: "attributes_bool_value",
				Type: CompoundType{
					Name: "Array",
					BaseType: BaseType{
						Name:   "Bool",
						goType: NewBaseType("Bool").goType,
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"attributes_string_key": &Column{
				Name: "attributes_string_key",
				Type: CompoundType{
					Name: "Array",
					BaseType: BaseType{
						Name:   "String",
						goType: NewBaseType("String").goType,
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"source": &Column{
				Name: "source",
				Type: BaseType{
					Name:   "String",
					goType: NewBaseType("String").goType,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"timestamp": &Column{
				Name: "timestamp",
				Type: BaseType{
					Name:   "DateTime64",
					goType: nil,
				},
				Codec: Codec{
					Name: "",
				},
			},
			"attributes_float64_value": &Column{
				Name: "attributes_float64_value",
				Type: CompoundType{
					Name: "Array",
					BaseType: BaseType{
						Name:   "Float64",
						goType: NewBaseType("Float64").goType,
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"attributes_string_value": &Column{
				Name: "attributes_string_value",
				Type: CompoundType{
					Name: "Array",
					BaseType: BaseType{
						Name:   "String",
						goType: NewBaseType("String").goType,
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"attributes_bool_key": &Column{
				Name: "attributes_bool_key",
				Type: CompoundType{
					Name: "Array",
					BaseType: BaseType{
						Name:   "String",
						goType: NewBaseType("String").goType,
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
			"attributes_float64_key": &Column{
				Name: "attributes_float64_key",
				Type: CompoundType{
					Name: "Array",
					BaseType: BaseType{
						Name:   "String",
						goType: NewBaseType("String").goType,
					},
				},
				Codec: Codec{
					Name: "",
				},
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:                          true,
			timestampDefaultsNow:                  true,
			engine:                                "MergeTree",
			orderBy:                               "(timestamp)",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "",
			hasOthers:                             false,
			attributes:                            []Attribute{},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
	},
}
