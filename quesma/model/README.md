# Aggregations support list

In Elasticsearch there are 3 types of aggregations: metrics, bucket, and pipelines. He's a list of all of them with our level of support<br>
More info: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html

| Legend:            | <!-- -->                                                                                                                                                                                                                                                                       |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| :white_check_mark: | Supported either <b>fully, or almost fully </b>and adding missing features shouldn't be hard.<br><b>WARNING:</b> even simplest aggregations may have their own more complex features, like supporting custom scripts written in Javascript. That isn't easy to do for any of the aggregations. |
| :wavy_dash:        | <b>Partially</b> supported.                                                                                                                                                                                                                                                    |
| :x:                | Completely <b>not</b> supported.                                                                                                                                                                                                                                               |

 Metrics aggregation       |      Support       | Bucket aggregation           |      Support       | Pipeline aggregation   |      Support       |
---------------------------|:------------------:|------------------------------|:------------------:|------------------------|:------------------:|
 Avg                       | :white_check_mark: | Adjacency matrix             |        :x:         | Average bucket         |        :x:         |
 Cardinality               | :white_check_mark: | Auto-interval date histogram |        :x:         | Bucket script          |        :x:         |
 Extended Stats            |        :x:         | Categorize text              |        :x:         | Bucket count K-S test  |        :x:         |
 Avg                       | :white_check_mark: | Children                     |        :x:         | Bucket correlation     |        :x:         |
 Boxplot                   |        :x:         | Composite                    |        :x:         | Bucket selector        |        :x:         |
 Cardinality               | :white_check_mark: | Date histogram               | :white_check_mark: | Bucket sort            |        :x:         |
 Extended stats            |        :x:         | Date range                   | :white_check_mark: | Change point           |        :x:         |
 Geo-bounds                |        :x:         | Diversified sampler          |        :x:         | Cumulative cardinality |        :x:         |
 Geo-centroid              |        :x:         | Filter                       | :white_check_mark: | Cumulative sum         |        :x:         |
 Geo-line                  |        :x:         | Filters                      | :white_check_mark: | Derivative             |        :x:         |
 Cartesian-bounds          |        :x:         | Frequent item sets           |        :x:         | Extended stats bucket  |        :x:         |
 Cartesian-centroid        |        :x:         | Geo-distance                 |        :x:         | Inference bucket       |        :x:         |
 Matrix stats              |        :x:         | Geohash grid                 |        :x:         | Max bucket             |        :x:         |
 Max                       | :white_check_mark: | Geotile grid                 |        :x:         | Min bucket             |        :x:         |
 Median absolute deviation |        :x:         | Global                       |        :x:         | Moving function        |        :x:         |
 Min                       | :white_check_mark: | Histogram                    | :white_check_mark: | Moving percentiles     |        :x:         |
 Percentile ranks          | :white_check_mark: | IP prefix                    |        :x:         | Normalize              |        :x:         |
 Percentiles               | :white_check_mark: | IP range                     |        :x:         | Percentiles bucket     |        :x:         |
 Rate                      |        :x:         | Missing                      |        :x:         | Serial differencing    |        :x:         |
 Scripted metric           |        :x:         | Multi-terms                  |        :x:         | Stats bucket           |        :x:         |
 Stats                     | :white_check_mark: | Nested                       |        :x:         | Sum bucket             |        :x:         |
 String stats              |        :x:         | Parent                       |        :x:         |
 Sum                       | :white_check_mark: | Random sampler               |    :wavy_dash:     |
 T-test                    |        :x:         | Range                        | :white_check_mark: |
 Top hits                  | :white_check_mark: | Rare terms                   |        :x:         |
 Top metrics               | :white_check_mark: | Reverse nested               |        :x:         |
 Value count               | :white_check_mark: | Sampler                      |    :wavy_dash:     |
 Weighted avg              |        :x:         | Significant terms            | :white_check_mark: |
|                          |                    | Significant text             |        :x:         |
|                          |                    | Terms                        | :white_check_mark: |
|                          |                    | Time series                  |        :x:         |
|                          |                    | Variable width histogram     |        :x:         |
