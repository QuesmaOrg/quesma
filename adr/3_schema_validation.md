# Schema semantic verification

## Context and Problem Statement

The schema describes the structure of the data and the mapping between the Elastic representation and ClickHouse.
It encompasses the data types, constraints, and relationships between fields.
Quesma needs this information to build correct SQL queries.
A correct SQL query ensures, for instance, that it requests the right columns that exist in the destination database (ClickHouse) and applies transformations according to the schema.
This is also important from a performance perspective, as it allows for generating optimal queries and removing parts that are redundant or unnecessary.

This problem consists of two sub-problems:

Schema representation
Schema validation

This ADR is about the second part - schema validation except for some initial information needed
to correctly generate queries like field types, and indexes - tables mapping. This can be even hardcoded
and does not require to have fully specified schema representation.

## Considered Options

* Option A: Eager schema validation.

Eager schema validation means that we validate the schema as soon as possible.
This requires schema to be loaded into memory and used during query parsing.


* Option B: Lazy schema validation.

Build kind of logical query representation and postpone schema validation after parsing the query
and just before executing it. 
Recent changes in search architecture, where we separated the query parsing from the execution allow us to do that.
It's now easy to introduce intermediate step or steps between parsing and execution. 
Below snippet shows how interface could look like:

```go
type Transformer interface {
	Transform(query []model.Query) ([]model.Query, error)
}

func (s *SchemaCheckPass) Transform(query []model.Query) ([]model.Query, error) {
    return query, nil
}

type TransformationPipeline struct {
    transformers []Transformer
}

func (o *TransformationPipeline) Transform(query []model.Query) ([]model.Query, error) {
    for _, transformer := range o.transformers {
        query, _ = transformer.Transform(query)
    }
    return query, nil
}
```

Then we could execute schema validation and other transformations in the pipeline in the following way.

```go

transformationPipeline := TransformationPipeline{
    transformers: []Transformer{
        &SchemaCheckPass{},
    },
}

queries, columns, isAggregation, canParse, err := queryTranslator.ParseQuery(body)
queries, err = transformationPipeline.Transform(queries)
```

Steps needed to implement this option (Most of them can be parallelized):
1. Finish refactoring `SimpleQuery` representation to be more tree-like structure
2. Propagate above changes to `model.Query`.
3. Combine `SimpleQuery` and `model.Query` into one structure (nice to have, but does not seem critical at this point)
4. Get rid of looping over resolved clickhouse sources in search logic. This is kind of blocker
   to be able to handle M elastic indexes - N clickhouse tables mapping.
   Decision which table(s) we should take into account generating final queries should be part of schema validation step. 
5. Implement schema validation transformer that will take all the information available described in defined
   schema representation and apply it to the query.

Steps 3 and 4 require some more detailed planning and investigation.

## Decision Outcome and Drivers

Chosen option: "Option A", because

* Seems that it's easier to do all required transformations at one place having all the information available.
* It promotes pipeline architecture where we can easily add more transformations in the future.
* It's easier to test and reason about the code as we can test specific transformation in isolation.
* It requires an abstract query representation that should simplify future db engines support.

We rejected "Option B" because:

* It requires more changes in the current architecture and flow.
* It's harder to reason about the code and changes as it will touch multiple places to
  take schema validation into account.
* It's harder to test as we need to test the whole flow to be sure that schema validation is working correctly.
* It combines at least two responsibilities in one place - parsing and schema validation.

## People
- @pdelewski 
- @jakozaur
