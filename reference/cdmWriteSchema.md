# Get cdm write schema

Get cdm write schema

## Usage

``` r
cdmWriteSchema(cdm)
```

## Arguments

- cdm:

  A cdm reference object created by `cdmFromCon`

## Value

The database write schema

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())

cdm <- cdmFromCon(con = con, cdmName = "Eunomia",
                  cdmSchema =  "main", writeSchema = "main")

cdmWriteSchema(cdm)

DBI::dbDisconnect(con)
} # }
```
