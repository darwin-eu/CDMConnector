# Add attrition reason to a cohort_table object

Update the cohort attrition table with new counts and a reason for
attrition.

## Usage

``` r
record_cohort_attrition(cohort, reason, cohortId = NULL)
```

## Arguments

- cohort:

  A generated cohort set

- reason:

  The reason for attrition as a character string

- cohortId:

  Cohort definition id of the cohort you want to update the attrition

## Value

The cohort object with the attributes created or updated.

**\[deprecated\]**
