# List the available example CDM datasets

List the available example CDM datasets

## Usage

``` r
exampleDatasets()
```

## Value

A character vector with example CDM dataset identifiers

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
exampleDatasets()[1]
#> [1] "GiBleed"

con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("GiBleed"))
cdm <- cdmFromCon(con)
} # }
```
