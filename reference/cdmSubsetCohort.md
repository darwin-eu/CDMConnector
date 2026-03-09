# Subset a cdm to the individuals in one or more cohorts

`cdmSubset` will return a new cdm object that contains lazy queries
pointing to each of the cdm tables but subset to individuals in a
generated cohort. Since the cdm tables are lazy queries, the subset
operation will only be done when the tables are used. `computeQuery` can
be used to run the SQL used to subset a cdm table and store it as a new
table in the database.

## Usage

``` r
cdmSubsetCohort(cdm, cohortTable = "cohort", cohortId = NULL, verbose = FALSE)
```

## Arguments

- cdm:

  A cdm_reference object

- cohortTable:

  The name of a cohort table in the cdm reference

- cohortId:

  IDs of the cohorts that we want to subset from the cohort table. If
  NULL (default) all cohorts in cohort table are considered.

- verbose:

  Should subset messages be printed? TRUE or FALSE (default)

## Value

A modified cdm_reference with all clinical tables subset to just the
persons in the selected cohorts.

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
library(dplyr, warn.conflicts = FALSE)

con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())

cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

# generate a cohort
path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")

cohortSet <- readCohortSet(path) %>%
  filter(cohort_name == "GIBleed_male")

# subset cdm to persons in the generated cohort
cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed")

cdmGiBleed <- cdmSubsetCohort(cdm, cohortTable = "gibleed")

cdmGiBleed$person %>%
  tally()
#> # Source:   SQL [1 x 1]
#> # Database: DuckDB 0.6.1
#>       n
#>   <dbl>
#> 1   237

cdm$person %>%
  tally()
#> # Source:   SQL [1 x 1]
#> # Database: DuckDB 0.6.1
#>       n
#>   <dbl>
#> 1  2694


DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
