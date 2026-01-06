# Get the CDM name

Extract the CDM name attribute from a cdm_reference object

## Usage

``` r
cdm_name(cdm)
```

## Arguments

- cdm:

  A cdm_reference object

## Value

The name of the CDM as a character string

## Details

**\[deprecated\]**

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
cdmName(cdm)
#> [1] "eunomia"

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
