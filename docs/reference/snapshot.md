# Extract CDM metadata

Extract the name, version, and selected record counts from a cdm.

## Usage

``` r
snapshot(cdm, computeDataHash = FALSE)
```

## Arguments

- cdm:

  A cdm object

- computeDataHash:

  Compute a hash of the CDM. See ?DatabaseConnector::computeDataHash for
  details.

## Value

A named list of attributes about the cdm including selected fields from
the cdm_source table and record counts from the person and
observation_period tables

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con, "main")
snapshot(cdm)

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
