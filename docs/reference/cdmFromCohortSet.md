# Build a Synthetic CDM from a Cohort Set

Constructs a synthetic OMOP Common Data Model (CDM) using a set of
cohort definitions, typically created using
[`CDMConnector::readCohortSet()`](readCohortSet.md). The function
generates synthetic data and returns a cdm reference object backed by a
DuckDB database, containing synthetic CDM tables and generated cohort
table rows.

## Usage

``` r
cdmFromCohortSet(cohortSet, n = 100)
```

## Arguments

- cohortSet:

  A data frame (usually from
  [`CDMConnector::readCohortSet()`](readCohortSet.md)) with columns
  `cohort_definition_id`, `cohort_name`, and `cohort` (cohort definition
  as a list or JSON string).

- n:

  Integer. Total number of synthetic persons to generate across all
  cohorts. Defaults to 100.

## Value

A cdm reference object (as returned by
[`CDMConnector::cdmFromCon()`](cdmFromCon.md)) backed by a DuckDB
database. The returned object contains synthetic CDM tables and cohort
table rows generated from the specified cohort definitions.

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
cohortSet <- readCohortSet(system.file("cohorts", package = "CDMConnector"))
cdm <- cdmFromCohortSet(cohortSet, n = 100)
cdm$person
} # }
```
