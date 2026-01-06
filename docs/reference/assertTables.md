# Assert that tables exist in a cdm object

A cdm object is a list of references to a subset of tables in the OMOP
Common Data Model. If you write a function that accepts a cdm object as
a parameter `assert_tables`/`assertTables` will help you check that the
tables you need are in the cdm object, have the correct columns/fields,
and (optionally) are not empty.

## Usage

``` r
assertTables(cdm, tables, empty.ok = FALSE, add = NULL)

assert_tables(cdm, tables, empty.ok = FALSE, add = NULL)
```

## Arguments

- cdm:

  A cdm object

- tables:

  A character vector of table names to check.

- empty.ok:

  Should an empty table (0 rows) be considered an error? TRUE or FALSE
  (default)

- add:

  An optional AssertCollection created by
  [`checkmate::makeAssertCollection()`](https://mllg.github.io/checkmate/reference/AssertCollection.html)
  that errors should be added to.

## Value

Invisibly returns the cdm object

## Details

**\[deprecated\]**

## Examples

``` r
if (FALSE) { # \dontrun{
# Use assertTables inside a function to check that tables exist
countDrugsByGender <- function(cdm) {
  assertTables(cdm, tables = c("person", "drug_era"), empty.ok = FALSE)

  cdm$person %>%
    dplyr::inner_join(cdm$drug_era, by = "person_id") %>%
    dplyr::count(.data$gender_concept_id, .data$drug_concept_id) %>%
    dplyr::collect()
}

library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
cdm <- cdmFromCon(con)

countDrugsByGender(cdm)

DBI::dbDisconnect(con, shutdown = TRUE)

} # }
```
