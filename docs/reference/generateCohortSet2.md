# Generate a cohort set on a CDM object (optimized, no Java dependency)

Generates cohort sets using an optimized DAG-based SQL pipeline that
does not require Java or CirceR. This is a faster alternative to
[`generateCohortSet`](generateCohortSet.md) that produces equivalent
results.

A "cohort_table" object consists of four components:

- A remote table reference to an OHDSI cohort table with columns:
  cohort_definition_id, subject_id, cohort_start_date, cohort_end_date.

- A **settings attribute** containing cohort settings including names.

- An **attrition attribute** with attrition information recorded during
  generation. This attribute is optional. Since calculating attrition
  takes additional compute it can be skipped resulting in a NULL
  attrition attribute.

- A **cohortCounts attribute** containing cohort counts.

## Usage

``` r
generateCohortSet2(
  cdm,
  cohortSet,
  name,
  computeAttrition = TRUE,
  overwrite = TRUE,
  cache = FALSE
)
```

## Arguments

- cdm:

  A cdm reference created by CDMConnector. write_schema must be
  specified.

- cohortSet:

  A cohortSet dataframe created with
  [`readCohortSet`](readCohortSet.md).

- name:

  Name of the cohort table to be created. This will also be used as a
  prefix for the cohort attribute tables. Must be a lowercase character
  string that starts with a letter and only contains letters, numbers,
  and underscores.

- computeAttrition:

  Should attrition be computed? TRUE (default) or FALSE

- overwrite:

  Should the cohort table be overwritten if it already exists? TRUE
  (default) or FALSE

- cache:

  Logical; if TRUE, enable incremental DAG caching. Previously computed
  intermediate tables are reused, and only changed portions of the DAG
  are recomputed. Default FALSE.

## Value

A cdm reference with the generated cohort table added.

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con,
                  cdmSchema = "main",
                  writeSchema = "main")

cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
cdm <- generateCohortSet2(cdm, cohortSet, name = "cohort")

print(cdm$cohort)

attrition(cdm$cohort)
settings(cdm$cohort)
cohortCount(cdm$cohort)
} # }
```
