# Intersect all cohorts in a single cohort table

Intersect all cohorts in a single cohort table

## Usage

``` r
intersect_cohorts(x, cohort_definition_id = 1L)

intersectCohorts(x, cohort_definition_id = 1L)
```

## Arguments

- x:

  A tbl reference to a cohort table

- cohort_definition_id:

  A number to use for the new cohort_definition_id

  **\[superseded\]**

## Value

A lazy query that when executed will resolve to a new cohort table with
one cohort_definition_id resulting from the intersection of all cohorts
in the original cohort table
