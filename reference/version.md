# Get the CDM version

Extract the CDM version attribute from a cdm_reference object

## Usage

``` r
version(cdm)
```

## Arguments

- cdm:

  A cdm object

## Value

"5.3" or "5.4"

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
version(cdm)

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
