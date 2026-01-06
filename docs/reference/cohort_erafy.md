# Collapse cohort records within a certain number of days

Collapse cohort records within a certain number of days

## Usage

``` r
cohort_erafy(x, gap)

cohortErafy(x, gap)
```

## Arguments

- x:

  A generated cohort set

- gap:

  When two cohort records are 'gap' days apart or less the periods will
  be collapsed into a single record

## Value

A lazy query on a generated cohort set
