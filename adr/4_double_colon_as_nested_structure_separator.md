# Use Double Colon (`::`) as a Separator for Nested Fields Representation in Clickhouse

## Context and Problem Statement

When ingesting JSON documents, representing nested objects in ClickHouse requires special handling - nested objects get flattened into columns. 
To distinguish between nested fields, a separator is needed. The most natural choice would be a dot (`.`), but ClickHouse treats it as a special character used for dealing with `Nested` columns, which can lead to parsing errors or unexpected behavior even when properly escaped.

For example, consider the following `CREATE TABLE`:
```sql
CREATE TABLE test (
    `event.type` Array(String),
    `event.name` Array(String),
)
```

When ingesting JSON documents with the following structure:
```json
{
    "event": {
        "type": ["A", "B"],
        "name": ["X"]
    }
}
```

ClickHouse will assume that `event` is a nested object and expect arrays of the same size to ensure correct representation. As much as this sounds useful, handling empty or optional values becomes cumbersome.

Another example:
> Dot in column names has a special/reserved meaning. CH expects that columns with dot is Arrays.
> Do not use dot.

source: [link](https://github.com/ClickHouse/ClickHouse/issues/18765#issuecomment-754661913)


### Considered Options

1. **Using Dot (`.`) as a Separator**
    - Pros:
        - A natural and obvious solution.
        - Familiar to users.
    - Cons:
        - Doesn't work.
2. **Using Double Colon (`::`) as a Separator**
    - Pros:
        - Avoids conflict with ClickHouse's treatment of `.`.
        - Clear and unambiguous representation of nested fields.
        - Simplifies query generation and ensures correct parsing by ClickHouse.
        - Rarely used in field names, reducing the risk of conflicts.
    - Cons:
        - Less common and may require initial adaptation by users.
        - Requires extra effort to expose those to users as dots.

### Decision Outcome and Drivers

**Chosen Option:** Using Double Colon (`::`) as a Separator because:
- **Avoids conflicts:** By using `::`, we avoid the special treatment of `.` in ClickHouse, ensuring that nested fields are parsed and interpreted correctly without additional processing.
- **No better alternatives exist:** Every separator that is not `.` will have more/less same cons as `::` and there's nothing we can do about it for now.

We rejected the option of using `.` because it didn't work.

## People

- @pivovarit
