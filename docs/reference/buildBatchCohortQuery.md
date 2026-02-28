# Build optimized batch cohort SQL using the execution DAG

Builds an execution graph (DAG) from cohort definitions, deduplicates
shared computation nodes, and emits a single SQL script with
schema-qualified prefixed working tables (no temp tables).

## Usage

``` r
buildBatchCohortQuery(
  cohort_list,
  cohort_ids,
  options = list(),
  cache = FALSE,
  con = NULL,
  schema = NULL
)
```

## Arguments

- cohort_list:

  List of cohort expression lists (from cohortExpressionFromJson).

- cohort_ids:

  Integer vector, same length as cohort_list.

- options:

  List with cdm_schema, vocabulary_schema, results_schema, table_prefix.

- cache:

  Logical; if TRUE, enable incremental caching (default FALSE).

- con:

  DBI connection; required when `cache = TRUE`.

- schema:

  Character resolved schema name; required when `cache = TRUE`.

## Value

When `cache = FALSE`: single SQL string. When `cache = TRUE`: list with
`sql`, `dag`, `cache_hits`, `cache_misses`.

## Details

When `cache = TRUE` and a DBI connection is provided, the function
checks a persistent cache registry for previously computed nodes. Nodes
whose content hash already exists in the cache are skipped, and only new
or changed nodes are materialized.
