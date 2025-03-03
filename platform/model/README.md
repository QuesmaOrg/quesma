# Aggregations support list

In Elasticsearch there are 3 types of aggregations: metrics, bucket, and pipelines. He's a list of all of them with our level of support<br>
More info: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html

| Legend:            | <!-- -->                                                                                                                                                                                                                                                                       |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| :white_check_mark: | Supported either <b>fully, or almost fully </b>and adding missing features shouldn't be hard.<br><b>WARNING:</b> even simplest aggregations may have their own more complex features, like supporting custom scripts written in Javascript. That isn't easy to do for any of the aggregations. |
| :wavy_dash:        | <b>Partially</b> supported.                                                                                                                                                                                                                                                    |
| :x:                | Completely <b>not</b> supported.                                                                                                                                                                                                                                               |

 Metrics aggregation       |        Support         | Bucket aggregation           |      Support       | Pipeline aggregation   |      Support       |
---------------------------|:----------------------:|------------------------------|:------------------:|------------------------|:------------------:|
 Avg                       |   :white_check_mark:   | Adjacency matrix             |        :x:         | Average bucket         | :white_check_mark: |
 Cardinality               |   :white_check_mark:   | Auto-interval date histogram |    :wavy_dash:     | Bucket script          |     :wavy_dash:    |
 Extended Stats            | :white_check_mark:[^1] | Categorize text              |        :x:         | Bucket count K-S test  |        :x:         |
 Avg                       |   :white_check_mark:   | Children                     |        :x:         | Bucket correlation     |        :x:         |
 Boxplot                   |          :x:           | Composite                    | :white_check_mark: | Bucket selector        |        :x:         |
 Cardinality               |   :white_check_mark:   | Date histogram               | :white_check_mark: | Bucket sort            |        :x:         |
 Extended stats            | :white_check_mark:[^1] | Date range                   | :white_check_mark: | Change point           |        :x:         |
 Geo-bounds                |          :x:           | Diversified sampler          |        :x:         | Cumulative cardinality |        :x:         |
 Geo-centroid              |          :x:           | Filter                       | :white_check_mark: | Cumulative sum         | :white_check_mark: |
 Geo-line                  |          :x:           | Filters                      | :white_check_mark: | Derivative             | :white_check_mark: |
 Cartesian-bounds          |          :x:           | Frequent item sets           |        :x:         | Extended stats bucket  |        :x:         |
 Cartesian-centroid        |          :x:           | Geo-distance                 |        :x:         | Inference bucket       |        :x:         |
 Matrix stats              |          :x:           | Geohash grid                 | :white_check_mark: | Max bucket             | :white_check_mark: |
 Max                       |   :white_check_mark:   | Geotile grid                 |        :x:         | Min bucket             | :white_check_mark: |
 Median absolute deviation |          :x:           | Global                       |        :x:         | Moving function        |    :wavy_dash:     |
 Min                       |   :white_check_mark:   | Histogram                    | :white_check_mark: | Moving percentiles     |        :x:         |
 Percentile ranks          |   :white_check_mark:   | IP prefix                    | :white_check_mark: | Normalize              |        :x:         |
 Percentiles               |   :white_check_mark:   | IP range                     | :white_check_mark: | Percentiles bucket     |        :x:         |
 Rate                      |          :x:           | Missing                      |        :x:         | Serial differencing    | :white_check_mark: |
 Scripted metric           |          :x:           | Multi-terms                  | :white_check_mark: | Stats bucket           |        :x:         |
 Stats                     |   :white_check_mark:   | Nested                       |        :x:         | Sum bucket             | :white_check_mark: |
 String stats              |          :x:           | Parent                       |        :x:         |
 Sum                       |   :white_check_mark:   | Random sampler               |    :wavy_dash:     |
 T-test                    |          :x:           | Range                        | :white_check_mark: |
 Top hits                  |   :white_check_mark:   | Rare terms                   |        :x:         |
 Top metrics               |   :white_check_mark:   | Reverse nested               |        :x:         |
 Value count               |   :white_check_mark:   | Sampler                      |    :wavy_dash:     |
 Weighted avg              |          :x:           | Significant terms            | :white_check_mark: |
|                          |                        | Significant text             |        :x:         |
|                          |                        | Terms                        | :white_check_mark: |
|                          |                        | Time series                  |        :x:         |
|                          |                        | Variable width histogram     |        :x:         |

[^1]: only `missing` parameter isn't supported, but it seems like a very unlikely use case.

# Query DSL support list

Query DSL language is the main one used in Kibana queries. AFAIK it's only responsible for filtering documents.<br>
More info: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html

 Compound queries |      Support       | Geo queries      | Support | Joining queries |      Support       | Other          |      Support       |
|-----------------|:------------------:|------------------|:-------:|-----------------|:------------------:|----------------|:------------------:|
| Boolean         | :white_check_mark: | Geo-bounding box |   :x:   | Nested          | :white_check_mark: | Match all      | :white_check_mark: |
| Boosting        |        :x:         | Geo-distance     |   :x:   | Has child       |        :x:         | Match none     |        :x:         | 
| Constant score  |        :x:         | Geo-grid         |   :x:   | Has parent      |        :x:         | Text expansion |        :x:         |
| Disjunction max |        :x:         | Geo-polygon      |   :x:   | Parent ID       |        :x:         | Shape          |        :x:         |
| Function score  |        :x:         | Geoshape         |   :x:   |

 Full text queries     |      Support       | Span queries       | Support | Specialized queries  | Support | Term-level queries |      Support       |
|----------------------|:------------------:|--------------------|:-------:|----------------------|:-------:|--------------------|:------------------:|
| Intervals            |        :x:         | Span containing    |   :x:   | Distance feature     |   :x:   | Exists             | :white_check_mark: |
| Match                | :white_check_mark: | Span field masking |   :x:   | More like this       |   :x:   | Fuzzy              |        :x:         |
| Match boolean prefix |        :x:         | Span first         |   :x:   | Percolate            |   :x:   | IDs                | :white_check_mark: |
| Match phrase         | :white_check_mark: | Span multi-term    |   :x:   | Knn                  |   :x:   | Prefix             | :white_check_mark: |
| Match phrase prefix  |        :x:         | Span near          |   :x:   | Rank feature         |   :x:   | Range              | :white_check_mark: |
| Combined fields      |        :x:         | Span not           |   :x:   | Script               |   :x:   | Regexp             | :white_check_mark: |
| Multi-match          | :white_check_mark: | Span or            |   :x:   | Script score         |   :x:   | Term               | :white_check_mark: |
| Query string         | :white_check_mark: | Span term          |   :x:   | Wrapper              |   :x:   | Terms              | :white_check_mark: |
| Simple query string  |    :wavy_dash:     | Span within        |   :x:   | Pinned Query         |   :x:   | Terms set          |        :x:         |
|                      |                    |                    |         | Rule                 |   :x:   | Wildcard           | :white_check_mark: |
|                      |                    |                    |         | Weighted tokens      |   :x:   |