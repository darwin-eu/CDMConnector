# Helper for working with compound schemas

This is similar to dbplyr::in_schema but has been tested across multiple
database platforms. It only exists to work around some of the
limitations of dbplyr::in_schema.

## Usage

``` r
inSchema(schema, table, dbms = NULL)
```

## Arguments

- schema:

  A schema name as a character string

- table:

  A table name as character string

- dbms:

  The name of the database management system as returned by
  `dbms(connection)`

## Value

A DBI::Id that represents a qualified table and schema
