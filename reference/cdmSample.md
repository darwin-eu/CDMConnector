# Subset a cdm object to a random sample of individuals

`cdmSample` takes a cdm object and returns a new cdm that includes only
a random sample of persons in the cdm. Only `person_id`s in both the
person table and observation_period table will be considered.

## Usage

``` r
cdmSample(cdm, n, seed = sample.int(1e+06, 1), name = "person_sample")
```

## Arguments

- cdm:

  A cdm_reference object.

- n:

  Number of persons to include in the cdm.

- seed:

  Seed for the random number generator.

- name:

  Name of the table that will contain the sample of persons.

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

cdmSampled <- cdmSample(cdm, n = 2)

cdmSampled$person %>%
  select(person_id)
#> # Source:   SQL [2 x 1]
#> # Database: DuckDB 0.6.1
#>   person_id
#>       <dbl>
#> 1       155
#> 2      3422

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
