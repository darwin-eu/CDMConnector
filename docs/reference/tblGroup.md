# CDM table selection helper

The OMOP CDM tables are grouped together and the `tblGroup` function
allows users to easily create a CDM reference including one or more
table groups.

## Usage

``` r
tblGroup(group)
```

## Arguments

- group:

  A character vector of CDM table groups: "vocab", "clinical", "all",
  "default", "derived".

## Value

A character vector of CDM tables names in the groups

## Details

The "default" table group is meant to capture the most commonly used set
of CDM tables. Currently the "default" group is: person,
observation_period, visit_occurrence, visit_detail,
condition_occurrence, drug_exposure, procedure_occurrence,
device_exposure, measurement, observation, death, note, note_nlp,
specimen, fact_relationship, location, care_site, provider,
payer_plan_period, cost, drug_era, dose_era, condition_era, concept,
vocabulary, concept_relationship, concept_ancestor, concept_synonym,
drug_strength

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname = "cdm",
                      host = "localhost",
                      user = "postgres",
                      password = Sys.getenv("PASSWORD"))

cdm <- cdmFromCon(con, cdmName = "test", cdmSchema = "public") %>%
  cdmSelectTbl(tblGroup("vocab"))
} # }
```
