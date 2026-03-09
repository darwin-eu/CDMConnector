# Get underlying database connection

Get underlying database connection

## Usage

``` r
cdmCon(cdm)
```

## Arguments

- cdm:

  A cdm reference object created by `cdmFromCon`

## Value

A reference to the database containing tables in the cdm reference

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())

cdm <- cdmFromCon(con = con, cdmName = "Eunomia",
                  cdmSchema =  "main", writeSchema = "main")

cdmCon(cdm)

DBI::dbDisconnect(con)
} # }
```
