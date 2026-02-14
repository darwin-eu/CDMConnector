# Create a source for a cdm in a database.

Create a source for a cdm in a database.

## Usage

``` r
dbSource(con, writeSchema)
```

## Arguments

- con:

  Connection to a database.

- writeSchema:

  Schema where cohort tables are. If provided must have read and write
  access to it. If NULL the cdm will be created without a write_schema.
