# Get cohort counts from a generated_cohort_set object.

**\[deprecated\]**

## Usage

``` r
cohort_count(cohort)
```

## Arguments

- cohort:

  A generated_cohort_set object.

## Value

A table with the counts.

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
library(dplyr)

con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main")
cdm <- generateConceptCohortSet(
  cdm = cdm, conceptSet = list(pharyngitis = 4112343), name = "new_cohort"
)
cohortCount(cdm$new_cohort)
} # }
```
