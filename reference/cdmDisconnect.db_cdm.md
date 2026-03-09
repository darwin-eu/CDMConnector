# Disconnect the connection of the cdm object

This function will disconnect from the database as well as drop
"temporary" tables that were created on database systems that do not
support actual temporary tables. Currently temp tables are emulated on
Spark/Databricks systems.

## Usage

``` r
# S3 method for class 'db_cdm'
cdmDisconnect(cdm, dropPrefixTables = FALSE, ...)
```

## Arguments

- cdm:

  cdm reference

- dropPrefixTables:

  Whether to drop tables in the writeSchema prefixed with `writePrefix`

- ...:

  Not used. Included for compatibility with generic.
