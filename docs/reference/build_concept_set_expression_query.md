# Build concept set expression query

Generates SQL for a concept set expression (include/exclude,
descendants, mapped).

## Usage

``` r
build_concept_set_expression_query(
  expression,
  vocabulary_schema = "@vocabulary_database_schema"
)
```

## Arguments

- expression:

  List - ConceptSetExpression with items

- vocabulary_schema:

  Schema for vocabulary tables (default: @vocabulary_database_schema)

## Value

Character SQL query
