# Build a Synthetic CDM from a Cohort Set

Constructs a synthetic OMOP Common Data Model (CDM) using a set of
cohort definitions, created using
[`CDMConnector::readCohortSet()`](readCohortSet.md). The function
generates synthetic data and returns a cdm reference object backed by a
DuckDB database, containing synthetic CDM tables and generated cohort
table rows.

## Usage

``` r
cdmFromCohortSet(
  cohortSet,
  n = 100,
  cohortTable = "cohort",
  duckdbPath = NULL,
  seed = 1,
  verbose = FALSE,
  ...
)
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

- cohortTable:

  Character. Name of the cohort table (default `"cohort"`).

- duckdbPath:

  Character or NULL. Path for the final merged DuckDB; if NULL a
  temporary file is used.

- seed:

  Integer. Base RNG seed; each cohort uses `seed + cohort_index` for
  reproducibility (default 1).

- verbose:

  If TRUE, print progress per cohort and per attempt (default FALSE).

- ...:

  Arguments passed through to `cdmFromJson` for each cohort (e.g.
  `targetMatch`, `successRate`, `visitConceptId`, `eventDateJitter`,
  `visitDateJitter`, `demographicVariety`, `sourceAndTypeVariety`,
  `valueVariety`). `seed` is overridden per cohort. `targetMatch` is per
  cohort: that fraction of each cohort's generated persons are intended
  to qualify for that cohort only.

## Value

A cdm reference object (as returned by
[`CDMConnector::cdmFromCon()`](cdmFromCon.md)) backed by a DuckDB
database. The returned object contains synthetic CDM tables and cohort
table rows generated from the specified cohort definitions. The returned
`cdm` has an attribute `synthetic_summary` (a list with
`cohort_summaries`, `cohort_index`, `n_cohorts`, `summary` (one-line
text), `any_low_match`) for diagnostics and match rates.

## Reproducibility

With the same `seed`, `cohortSet`, and other arguments,
`cdmFromCohortSet` produces the same synthetic data. Changing `seed` or
`n` changes the data. The data are random but reproducible.

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
cohortSet <- readCohortSet(system.file("cohorts", package = "CDMConnector"))
cdm <- cdmFromCohortSet(cohortSet, n = 100)
cdm$person
} # }
```
