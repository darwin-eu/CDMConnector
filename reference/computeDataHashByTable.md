# Compute a hash for each CDM table

Compute a hash for each CDM table

## Usage

``` r
computeDataHashByTable(cdm)
```

## Arguments

- cdm:

  A cdm_reference object created by `cdmFromCon`

## Value

A dataframe with one row per table, row counts, unique value counts for
one column, and a hash

## Details

This function is used to track changes in CDM databases. It returns a
dataframe with one hash for each table. The hash is based on the overall
row count and the number of unique values of one column of the table.
For clinical tables we count the number of unique concept IDs. For some
tables we do not calculate any unique value count (e.g. the location
table) and simply use the total row count.

\`r lifecycle::badge("experimental")

## Examples

``` r
if (FALSE) { # \dontrun{
 library(CDMConnector)
 con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
 cdm <- cdmFromCon(con, "main", "main")
 computeDataHashByTable(cdm)
 cdmDisconnect(cdm)
} # }
```
