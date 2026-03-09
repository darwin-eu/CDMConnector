# Helper for working with compound schema

Helper for working with compound schema

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
