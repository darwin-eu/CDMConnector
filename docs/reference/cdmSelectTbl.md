# Select a subset of tables in a cdm reference object

This function uses syntax similar to
[`dplyr::select`](https://dplyr.tidyverse.org/reference/select.html) and
can be used to subset a cdm reference object to a specific tables

## Usage

``` r
cdmSelectTbl(cdm, ...)

cdm_select_tbl(cdm, ...)
```

## Arguments

- cdm:

  A cdm reference object created by `cdm_from_con`

- ...:

  One or more table names of the tables of the `cdm` object.
  `tidyselect` is supported, see
  [`dplyr::select()`](https://dplyr.tidyverse.org/reference/select.html)
  for details on the semantics.

## Value

A cdm reference object containing the selected tables

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())

cdm <- cdmFromCon(con, "main")

cdmSelectTbl(cdm, person)
cdmSelectTbl(cdm, person, observation_period)
cdmSelectTbl(cdm, tblGroup("vocab"))
cdmSelectTbl(cdm, "person")

DBI::dbDisconnect(con)
} # }
```
