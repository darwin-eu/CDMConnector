# Generate a cohort set on a cdm object

A "cohort_table" object consists of four components

- A remote table reference to an OHDSI cohort table with at least the
  columns: cohort_definition_id, subject_id, cohort_start_date,
  cohort_end_date. Additional columns are optional and some analytic
  packages define additional columns specific to certain analytic
  cohorts.

- A **settings attribute** which points to a remote table containing
  cohort settings including the names of the cohorts.

- An **attrition attribute** which points to a remote table with
  attrition information recorded during generation. This attribute is
  optional. Since calculating attrition takes additional compute it can
  be skipped resulting in a NULL attrition attribute.

- A **cohortCounts attribute** which points to a remote table containing
  cohort counts

Each of the three attributes are tidy tables. The implementation of this
object is experimental and user feedback is welcome.

**\[experimental\]** One key design principle is that cohort_table
objects are created once and can persist across analysis execution but
should not be modified after creation. While it is possible to modify a
cohort_table object doing so will invalidate it and it's attributes may
no longer be accurate.

## Usage

``` r
generateCohortSet(
  cdm,
  cohortSet,
  name,
  computeAttrition = TRUE,
  overwrite = TRUE
)
```

## Arguments

- cdm:

  A cdm reference created by CDMConnector. write_schema must be
  specified.

- cohortSet:

  A cohortSet dataframe created with
  [`readCohortSet()`](readCohortSet.md)

- name:

  Name of the cohort table to be created. This will also be used as a
  prefix for the cohort attribute tables. This must be a lowercase
  character string that starts with a letter and only contains letters,
  numbers, and underscores.

- computeAttrition:

  Should attrition be computed? TRUE (default) or FALSE

- overwrite:

  Should the cohort table be overwritten if it already exists? TRUE
  (default) or FALSE

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con,
                  cdmSchema = "main",
                  writeSchema = "main")

cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
cdm <- generateCohortSet(cdm, cohortSet, name = "cohort")

print(cdm$cohort)

attrition(cdm$cohort)
settings(cdm$cohort)
cohortCount(cdm$cohort)
} # }
```
