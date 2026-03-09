# Get the database management system (dbms) from a cdm_reference or DBI connection

Get the database management system (dbms) from a cdm_reference or DBI
connection

## Usage

``` r
dbms(con)
```

## Arguments

- con:

  A DBI connection or cdm_reference

## Value

A character string representing the dbms that can be used with SqlRender

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
cdm <- cdmFromCon(con)
dbms(cdm)
dbms(con)
} # }
```
