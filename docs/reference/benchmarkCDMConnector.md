# Run benchmark of tasks using CDMConnector

Run benchmark of tasks using CDMConnector

## Usage

``` r
benchmarkCDMConnector(cdm)
```

## Arguments

- cdm:

  A CDM reference object

## Value

a tibble with time taken for different analyses

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
benchmarkCDMConnector(cdm)

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
