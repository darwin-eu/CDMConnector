# Validation report for a CDM

Print a short validation report for a cdm object. The validation
includes checking that column names are correct and that no tables are
empty. A short report is printed to the console. This function is meant
for interactive use.

## Usage

``` r
validate_cdm(cdm)

validateCdm(cdm)
```

## Arguments

- cdm:

  A cdm reference object.

## Value

Invisibly returns the cdm input

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdm_from_con(con, cdm_schema = "main")
validate_cdm(cdm)
DBI::dbDisconnect(con)
} # }
```
