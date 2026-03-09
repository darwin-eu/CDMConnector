# List tables in a schema

DBI::dbListTables can be used to get all tables in a database but not
always in a specific schema. `listTables` will list tables in a schema.

## Usage

``` r
listTables(con, schema = NULL)
```

## Arguments

- con:

  A DBI connection to a database

- schema:

  The name of a schema in a database. If NULL, returns
  DBI::dbListTables(con).

## Value

A character vector of table names

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
listTables(con, schema = "main")
} # }
```
