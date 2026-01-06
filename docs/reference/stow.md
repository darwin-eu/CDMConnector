# Collect a list of lazy queries and save the results as files

Collect a list of lazy queries and save the results as files

## Usage

``` r
stow(cdm, path, format = "parquet")
```

## Arguments

- cdm:

  A cdm object

- path:

  A folder to save the cdm object to

- format:

  The file format to use: "parquet" (default), "csv", "feather" or
  "duckdb".

## Value

Invisibly returns the cdm input

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
vocab <- cdmFromCon(con, "main") %>%
  cdmSelectTbl("concept", "concept_ancestor")
stow(vocab, here::here("vocab_tables"))
DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
