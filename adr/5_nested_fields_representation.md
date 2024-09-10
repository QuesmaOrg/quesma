# Nested Fields Encoding and Representation 

## Context and Problem Statement

When ingesting JSON documents, representing nested objects in ClickHouse requires special handling - nested objects get flattened into columns. 
To distinguish between nested fields, we need mechanisms to represent them in a way that ensures correct parsing and representation in ClickHouse.

## Considered cases
Examples of valid names: `_1_`, `host_name`

Example of invalid names:
- `12_column` (started with number)
- `column-name` (contains `-`)
- `host.name` (contains `.`)
- `column name` (contains space)

## ElasticSearch Perspective

From the ElasticSeach perspective, two below cases are represented in the same way:
```json
{
   "a": {
      "b": 1
   }
}
```
```json
{
   "a.b": 1
}
```

Both will be stored as object kind of representation in ElasticSearch.
Dot in this case is just a separator.

### Considered Options

1. **Using Dot (`.`) as a Separator**
    - Pros:
        - A natural and obvious solution.
        - Familiar to ElasticSearch users.
    - Cons:
        - In ClickHouse, `.` is a special character that is used to access nested fields. This may lead to conflicts and parsing issues.
      Example:
      using `.` as an operator, below `size0`, `size1`, `size2` on arrays will not work.
        ```sql
        CREATE TABLE t_arr (`products.arr` Array(Array(Array(UInt32)))) ENGINE = MergeTree ORDER BY tuple();
        INSERT INTO t_arr VALUES ([[[12, 13, 0, 1],[12]]]);
        SELECT arr.size0, arr.size1, arr.size2 FROM t_arr;
        ```
      the same situation is with maps, we can use `.` as an operator to invoke functions on maps like `keys`
        ```sql
        CREATE TABLE table (map Map(String, String), "map.keys" Nullable(String)) ENGINE = Memory;
        INSERT INTO table VALUES (map('key', 'value', 'key2', 'value2'), 'test');
        SELECT mapKeys(map), map.keys, "map".keys, "map.keys" FROM table;
        ```
      We also made an experiment storing `.` as is, however ingest of `kibana_sample_data_ecommerce` failed
      with the following error:
      ```text
      message: Elements 'products.discount_amount' and 'products.taxful_price' of Nested data structure 'products' (Array columns) have different array sizes.
      ```
      So it seems that semantic of `.` matters and Clickhouse behaves differently seeing this separator.
      
2. **Using (`::`) as a Separator**
- Pros:
   - Avoids conflict with ClickHouse's treatment of `.`.
   - Clear and unambiguous representation of nested fields.
   - Rarely used in field names, reducing the risk of conflicts.
- Cons:
   - Less common and may require initial adaptation by users.
   - Requires extra effort to expose those to users as dots.
   - Requires quoting in SQL.
   - In some database `::` is used for casting which will confuse users.
  
3. **Using (`_`) as a Separator**
- Pros:
   - Feels more common to SQL and allows unquoted usage.
   - Avoid conflict with almost all SQL databases.
- Cons:
   - Very common in field names which could let to collisions.
   - For non-SQL it slightly less intuitive than `.`
  
4. **Using (`__`) as a Separator**
- Pros:
   - Feels more common to SQL and allows unquoted usage.
   - Avoid conflict with almost all SQL databases.
   - Less chance of collisions.
- Cons:
   - Users may need familiarize with it.
   - For non-SQL it slightly less intuitive than `.`.

5. **Non-conflicting encoding**
    - Pros:
        - Avoids conflicts with non-conflicting encoding.
    - Cons:
        - Requires additional processing to encode and decode nested fields.
        - Introduce of columns readability complexities. That's the price we need to pay for avoiding conflicts.

6. **Using simple encoding with ElasticSearch persistence storage**
    - Pros:
        - Avoids conflicts with special characters.
    - Cons:
        - Requires additional processing to encode and decode nested fields.
        - Requires external storage.
        - Potentially requires more code to handle storing/loading persistence information and additional calls to ElasticSearch.
        - May lead to some unexpected behavior when user move to another ElasticSearch.

7. **Using simple encoding with column comments (annotations) as persistent storage**
   - Pros:
      - Avoids conflicts with special characters.
      - Seems simpler than using external storage (option 5).
      - All information is stored in ClickHouse, so columns and its annotations (metadata) are connected.
   - Cons:
      - Requires additional processing to encode and decode nested fields.
      - Requires storing somewhere information about collisions (but seems much simpler than 5).

### Decision Outcome and Drivers

Options from 1 to 4 are based on choosing a separator for nested fields and may lead to conflicts with ClickHouse's syntax or other databases.
Option 5 is based on non-conflicting encoding of nested fields at the cost of column readability.
Option 6 and 7 is based on encoding nested fields to avoid conflicts.
When conflict happens, we are doing encoding and store information about it.

Options 6 and 7 might use the same encoding logic (below), but the difference is in the storage of the mapping.

Collision encoding logic:
   - all lower case
   - all non-alphanumeric are translated to ‘_’ (e.g. “host-name”, “host.name”, “host name” will be “host_name”)
   - if starts with digit, then add ‘_’ at beginning
   - Save mapping to persitent logic, on-collision do override.

As a result, we decided to use option 7 (which seems simpler) - using simple encoding with column comments (annotations) as persistent storage.

## People

- @pdelewski
- @mieciu
- @jakozaur
- @nablaone
