# Generate a cohort set on a local CDM (list of dataframes)

Copies the local CDM to an in-memory DuckDB database, runs
[`generateCohortSet`](generateCohortSet.md), then collects the generated
cohort table and its attributes back into R and adds them to the input
CDM.

## Usage

``` r
generateCohortSetLocal(
  cdm,
  cohortSet,
  name,
  computeAttrition = TRUE,
  overwrite = TRUE
)
```

## Arguments

- cdm:

  A local cdm object (list of dataframes, e.g. from
  `dplyr::collect(cdm)`).

- cohortSet:

  A cohort set from [`readCohortSet`](readCohortSet.md).

- name:

  Name of the cohort table to create.

- computeAttrition:

  Whether to compute attrition.

- overwrite:

  Whether to overwrite an existing cohort table.

## Value

The input `cdm` with the new cohort table added (as local dataframes).
