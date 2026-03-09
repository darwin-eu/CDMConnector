# Subset a cdm object to a set of persons

`cdmSubset` takes a cdm object and a list of person IDs as input. It
returns a new cdm that includes data only for persons matching the
provided person IDs. Generated cohorts in the cdm will also be subset to
the IDs provided.

## Usage

``` r
cdmSubset(cdm, personId)
```

## Arguments

- cdm:

  A cdm_reference object

- personId:

  A numeric vector of person IDs to include in the cdm

## Value

A modified cdm_reference object where all clinical tables are lazy
queries pointing to subset

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
library(dplyr, warn.conflicts = FALSE)

con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())

cdm <- cdmFromCon(con, cdmSchema = "main")

cdm2 <- cdmSubset(cdm, personId = c(2, 18, 42))

cdm2$person %>%
  select(1:3)
#> # Source:   SQL [3 x 3]
#> # Database: DuckDB 0.6.1
#>   person_id gender_concept_id year_of_birth
#>       <dbl>             <dbl>         <dbl>
#> 1         2              8532          1920
#> 2        18              8532          1965
#> 3        42              8532          1909

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
