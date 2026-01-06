# Union all cohorts in a cohort set with cohorts in a second cohort set

Union all cohorts in a cohort set with cohorts in a second cohort set

## Usage

``` r
cohort_union(x, y)

cohortUnion(x, y)
```

## Arguments

- x:

  A tbl reference to a cohort table with one or more generated cohorts

- y:

  A tbl reference to a cohort table with one generated cohort

## Value

A lazy query that when executed will resolve to a new cohort table with
one the same cohort_definitions_ids in x resulting from the union of all
cohorts in x with the single cohort in y cohort table
