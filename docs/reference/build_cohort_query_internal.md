# Build cohort query from cohort expression

Build cohort query from cohort expression

## Usage

``` r
build_cohort_query_internal(cohort, options = list())
```

## Arguments

- cohort:

  List - cohort expression (from cohort_expression_from_json)

- options:

  List - build options with cdm_schema, vocabulary_schema, target_table,
  cohort_id, etc.

## Value

List with `full_sql` (complete script), `tail_sql`,
`primary_events_sql`, `codeset_sql`, and other fragments for batch
assembly.
