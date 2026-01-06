# Bring a remote CDM reference into R

This function calls collect on a list of lazy queries and returns the
result as a list of dataframes.

## Usage

``` r
# S3 method for cdm_reference
collect(x, ...)
```

## Arguments

- x:

  A cdm_reference object.

- ...:

  Not used. Included for compatibility.

## Value

A cdm_reference object that is a list of R dataframes.

## Examples

``` r
if (FALSE) {
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
vocab <- cdm_from_con(con, "main") %>%
  cdm_select_tbl("concept", "concept_ancestor")

local_vocab <- collect(vocab)
DBI::dbDisconnect(con, shutdown = TRUE)
}
```
