EQL support
---


This package contains the EQL parser and query transformer. 

- The parser is generated using ANTLR4. The grammar is defined in `EQL.g4` file. The generated code is in `parser` directory. Do not review the generated code.
- HTTP endpoint is implemented in `FIXME`
- `query_translator.go` is the glue code that connects the parser with the Quesma search engine.
- Sample EQL query as an HTTP request is in `http_request/eql_search.http` file.
- A simple command line client is implemented in `playground` directory.
- End-to-End tests are implemented in `e2e` directory. See file `e2e/eql_test.go` for more details.


What is supported?
---

Comparison operators

| operator | supported          | comment |
|----------|--------------------|---------|
| `==`     | :heavy_check_mark: |         |
| `!=`     | :heavy_check_mark: |         |
| `>`      | :heavy_check_mark: |         |
| `>=`     | :heavy_check_mark: |         |
| `<`      | :heavy_check_mark: |         |
| `<=`     | :heavy_check_mark: |         | 
| `:`      | :heavy_check_mark: |         |


Lookup operators

| operator  | supported          | comment |
|-----------|--------------------|---------|
| `in`      | :heavy_check_mark: |         |
| `not in`  | :heavy_check_mark: |         |
| `in~`     | :heavy_check_mark: |         |
| `not in~` | :heavy_check_mark: |         |
| `:`       | :heavy_check_mark: |         |
| `like`    | :heavy_check_mark: |         |
| `like~`   | :heavy_check_mark: |         |
| `regex`   | :heavy_check_mark: |         |
| `regex~`  | :heavy_check_mark: |         |


Logical operators

| operator | supported          | comment |
|----------|--------------------|---------|
| `and`    | :heavy_check_mark: |         |
| `or`     | :heavy_check_mark: |         |
| `not`    | :heavy_check_mark: |         |



Supported functions


| function          | supported          | comment                                |
|-------------------|--------------------|----------------------------------------|
| `add`             | :heavy_check_mark: |                                        |
| `between`         | :x:                |                                        |
| `cidrMatch`       | :cockroach:        |                                        |
| `concat`          | :heavy_check_mark: |                                        |
| `divide`          | :cockroach:        | division of integers should be rounded |
| `endsWith`        | :heavy_check_mark: |                                        |
| `endsWith~`       | :heavy_check_mark: |                                        |
| `indexOf`         | :cockroach:        |                                        |
| `indexOf~`        | :cockroach:        |                                        |
| `length`          | :heavy_check_mark: |                                        |
| `modulo`          | :heavy_check_mark: |                                        |
| `multiply`        | :heavy_check_mark: |                                        |
| `number`          | :cockroach:        |                                        |
| `startsWith`      | :heavy_check_mark: |                                        |
| `startsWith~`     | :heavy_check_mark: |                                        |
| `string`          | :heavy_check_mark: |                                        |
| `stringContains`  | :cockroach:        |                                        |
| `stringContains~` | :cockroach:        |                                        |
| `substring`       | :cockroach:        |                                        |
| `subtract`        | :heavy_check_mark: |                                        |




Known EQL language limitations
---

1. We support only simple EQL queries. Sequence and sample queries are not supported.
2. Pipe operators are not supported. Syntax is parsed. Error is returned if pipe operator is used in the query. (https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-syntax.html#eql-pipes)
3. Optional fields are not supported. Field names are parsed. Error is returned if that field is used in the query. (https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-syntax.html#eql-syntax-optional-fields)
4. Backtick escaping is not supported. (https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-syntax.html#eql-syntax-escape-a-field-name)
5. Error handling is missing. Every error will be returned as na internal server error.


Kibana Alerts
---

Kibana alerts will not work at the moment. There is a few thing to do to make it work:
1. Implement a proper schema. `field_caps` must return the names with '.' separator for nested fields (for example `event.category`)
2. We should return a proper JSON response. Right now we are returning `hits`, we should return `events` collection instead. 
3. We should parse both `query` and `filter` fields. Right now we are parsing only `query` field. In other words we should combine KQL and EQL queries. See sample query below:
```
{
    "fields": [
        {
            "field": "*",
            "include_unmapped": true
        },
        {
            "field": "@timestamp",
            "format": "strict_date_optional_time"
        }
    ],
    "filter": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "format": "strict_date_optional_time",
                            "gte": "2024-05-10T15:15:18.622Z",
                            "lte": "2024-05-10T15:16:28.622Z"
                        }
                    }
                },
                {
                    "bool": {
                        "filter": [],
                        "must": [],
                        "must_not": [],
                        "should": []
                    }
                }
            ]
        }
    },
    "query": "process where process.name == \"quesma.exe\"",
    "runtime_mappings": {},
    "size": 100
}
```