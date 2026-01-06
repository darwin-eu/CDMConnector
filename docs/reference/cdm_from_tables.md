# Create a cdm object from local tables

**\[deprecated\]**

## Usage

``` r
cdm_from_tables(tables, cdm_name, cohort_tables = list(), cdm_version = NULL)
```

## Arguments

- tables:

  List of tables to be part of the cdm object.

- cdm_name:

  Name of the cdm object.

- cohort_tables:

  List of tables that contains cohort, cohort_set and cohort_attrition
  can be provided as attributes.

- cdm_version:

  Version of the cdm_reference

## Value

A `cdm_reference` object.

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)

person <- dplyr::tibble(
  person_id = 1, gender_concept_id = 0, year_of_birth = 1990,
  race_concept_id = 0, ethnicity_concept_id = 0
)
observation_period <- dplyr::tibble(
  observation_period_id = 1, person_id = 1,
  observation_period_start_date = as.Date("2000-01-01"),
  observation_period_end_date = as.Date("2025-12-31"),
  period_type_concept_id = 0
)
cdm <- cdmFromTables(
  tables = list("person" = person, "observation_period" = observation_period),
  cdmName = "test"
)
} # }
```
