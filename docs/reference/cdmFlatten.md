# Flatten a cdm into a single observation table

This experimental function transforms the OMOP CDM into a single
observation table. This is only recommended for use with a filtered CDM
or a cdm that is small in size.

## Usage

``` r
cdmFlatten(
  cdm,
  domain = c("condition_occurrence", "drug_exposure", "procedure_occurrence"),
  includeConceptName = TRUE
)
```

## Arguments

- cdm:

  A cdm_reference object

- domain:

  Domains to include. Must be a subset of "condition_occurrence",
  "drug_exposure", "procedure_occurrence", "measurement",
  "visit_occurrence", "death", "observation"

- includeConceptName:

  Should concept_name and type_concept_name be include in the output
  table? TRUE (default) or FALSE

## Value

A lazy query that when evaluated will result in a single table

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
library(dplyr, warn.conflicts = FALSE)

con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())

cdm <- cdmFromCon(con, cdmSchema = "main")

all_observations <- cdmSubset(cdm, personId = c(2, 18, 42)) %>%
  cdmFlatten() %>%
  collect()

all_observations
#> # A tibble: 213 × 8
#>    person_id observation_.  start_date end_date   type_.  domain obser.  type_.
#>        <dbl>          <dbl> <date>     <date>       <dbl> <chr>  <chr>   <chr>
#>  1         2       40213201 1986-09-09 1986-09-09  5.81e5 drug   pneumo  <NA>
#>  2        18        4116491 1997-11-09 1998-01-09  3.20e4 condi  Escher  <NA>
#>  3        18       40213227 2017-01-04 2017-01-04  5.81e5 drug   tetanu  <NA>
#>  4        42        4156265 1974-06-13 1974-06-27  3.20e4 condi  Facial  <NA>
#>  5        18       40213160 1966-02-23 1966-02-23  5.81e5 drug   poliov  <NA>
#>  6        42        4198190 1933-10-29 1933-10-29  3.80e7 proce  Append  <NA>
#>  7         2        4109685 1952-07-13 1952-07-27  3.20e4 condi  Lacera  <NA>
#>  8        18       40213260 2017-01-04 2017-01-04  5.81e5 drug   zoster  <NA>
#>  9        42        4151422 1985-02-03 1985-02-03  3.80e7 proce  Sputum  <NA>
#> 10         2        4163872 1993-03-29 1993-03-29  3.80e7 proce  Plain   <NA>
#> # ... with 203 more rows, and abbreviated variable names observation_concept_id,
#> #   type_concept_id, observation_concept_name, type_concept_name

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
