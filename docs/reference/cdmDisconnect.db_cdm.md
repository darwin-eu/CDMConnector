# Disconnect the connection of the cdm object

This function will disconnect from the database as well as drop
"temporary" tables that were created on database systems that do not
support actual temporary tables. Currently temp tables are emulated on
Spark/Databricks systems.

## Usage

``` r
# S3 method for class 'db_cdm'
cdmDisconnect(cdm, dropWriteSchema = FALSE, ...)
```

## Arguments

- cdm:

  cdm reference

- dropWriteSchema:

  Whether to drop tables in the writeSchema

- ...:

  Not used
