# Disallow Multiple Source Search

## Context and Problem Statement

Complex Elasticsearch index pattern algebra can lead to unexpected results when applied to multiple sources.

An index pattern is a string that matches one or more data streams, indices, or aliases.

Index pattern can:
- match a single source
  **`filebeat-a`**
- match multiple sources matching the wildcard pattern
  **`filebeat-*`**
- match multiple sources by combining multiple patterns
  **`filebeat-*,logs-*`**
- match multiple single source by listing them after a comma
  **`filebeat-a,filebeat-b`**
- exclude a source from a wider pattern by preceding it with a minus sign (-):
  **`filebeat-*,-filebeat-c`**
- for cross-cluster search, precede with the cluster name followed by a colon (:).
  **`cluster1:filebeat-*`**
  **`cluster1:filebeat-*,cluster2:filebeat-*`**
  **`cluster*:filebeat-*,filebeat-*`**

This makes it possible to search across multiple sources, but it can also lead to unexpected results when the pattern is too broad and includes remote sources that should not be searched together.

## Considered Options

1. Disallow multiple source search with small exceptions
2. Force the user to use cross-cluster search syntax to minimize the risk of unexpected multi-source search
3. Allow multiple source search

## Decision Outcome and Drivers

Chosen option: `1`, because:
* Allowing multiple source search would force us to implement database-engine logic, and we want to stick to being a gateway
* Forcing the user to user cross-cluster search syntax would force them to change all existing data views
* Kibana sometimes uses multiple source search, so we might want to cover some trivial cases

## People
- @ptbrzoska
- @pivovarit
- @jakozaur
