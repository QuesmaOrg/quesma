# Proposal: Use Double Colon (`__`) as a Separator for Nested Fields Representation

- Quesma's generated table or column names should be lowercased alphanumerical character and `_`.
  - All names must start with character or `_`.
- Names should not collide, even if database is case-sensitive.
- For nested fields, use `__` as a separator.
- We should store metadata about those translations, so it is backward compatible.

Examples of valid names: `_1_`, `host_name`

Example of invalid names:
- `12_column` (started with number)
- `column-name` (contains `-`)
- `host.name` (contains `.`)
- `column name` (contains space)

## Context and Problem Statement
ElasticSearch/OpenSearch accepts any JSON in field names:
```json
{
  "host.name": "Hostname",
  "something<>!!": 30,
  "cloud": {
    "name": "AWS",
    "region": "us-west-2"
  }
}
```
This is serialized into:
```json
{
  "host.name": ["Hostname"],
  "something<>!!": [30],
  "cloud.name": ["AWS"],
  "cloud.region": ["us-west-2"]
}
```
Dots are common with some headaches even for the [authors](https://github.com/elastic/elasticsearch/issues/63530).

We need to create corresponding SQL table. In SQL native names are limited to alphanumerical characters and `_`.

It's quite common to write SQL by hand like:
```sql
SELECT host.name, COUNT(*) FROM table GROUP BY host.name;
```
Which would result in error, as dot is a reserved operator.

This can be fixed by escaping:
```sql
SELECT "host.name", COUNT(*) FROM table GROUP BY "host.name";
```
It does not look nice and there are many bugs in tools and hidden limitations.

Example of bugs:
- [don't use dots in ClickHouse](https://github.com/ClickHouse/ClickHouse/issues/18765#issuecomment-754661913)
- [StarRocks doesn't allow some characters in column names](https://github.com/StarRocks/starrocks/issues/38854)
- [spaces caused bug in automation tool](https://community.n8n.io/t/error-invalid-name-syntax-for-columns-with-space/11757)

Also there are many limitations:
- [AWS Athena allows just certain characters in complex types](https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html#:~:text=%22%20FROM%20%22234table%22-,Column%20names%20and%20complex%20types,use%20a%20custom%20DDL%20statement.)

The common best practice is to use lowercase alphanumerical and underscores (e.g. [SiSense guide](https://www.sisense.com/blog/better-sql-schema/), [Baeldung](https://www.baeldung.com/sql/database-table-column-naming-conventions))
## Considered Options

For separator:

1. **Using Dot (`.`) as a Separator**
  - Pros:
    - A natural and obvious solution.
  - Cons:
      - Does not feel native to SQL   
      - We will hit bugs.
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

For escaping names:
1. **Don't escape name (`"some!<>"`)**
  - Pros:
    - Simple and intuitive
  - Cons:
    - Will hit bugs
    - May not even work in some cases
    - Doesn't feel native
    - Create extra burden when adding new DB
2. **Escape some characters, but not some (e.g. `.`)**
- Pros:
  - Some names would remain
- Cons:
  - Will hit bugs
  - May not even work in some cases
  - Doesn't feel native
  - Create extra burden when adding new DB
3. **Escape all characters, so names are always alphanumeric**
- Pros:
  - All names are SQL native
  - We can easily add more DBs
- Cons:
  - More work on Quesma's end, including handling collisions


## Decision Outcome and Drivers

To be added

# People

- @jakozaur


