# Validation report for a CDM

Print a short validation report for a cdm object. The validation
includes checking that column names are correct and that no tables are
empty. A short report is printed to the console. This function is meant
for interactive use.

## Usage

``` r
validateCdm(cdm)

validate_cdm(cdm)
```

## Arguments

- cdm:

  A cdm reference object.

## Value

Invisibly returns the cdm input

## Details

**\[deprecated\]**

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con, cdmSchema = "main")
validateCdm(cdm)
DBI::dbDisconnect(con)
} # }
```
