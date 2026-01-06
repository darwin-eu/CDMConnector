# Create an attrition diagram from a generated cohort set

Create an attrition diagram from a generated cohort set

## Usage

``` r
visr.omop_attrition(x, ...)
```

## Arguments

- x:

  A GeneratedCohortSet object

- ...:

  Not used

  **\[experimental\]**

## Value

No return value. This function will create one attrition plot for each
generated cohort.

## Examples

``` r
if (FALSE) {
library(CDMConnector)
library(dplyr)

con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdm_from_con(con, "main", "main")
cohort_set <- read_cohort_set(system.file("cohorts2", package = "CDMConnector"))
cdm <- generate_cohort_set(cdm, cohort_set, name = "cohort", overwrite = T)

cohort_attrition(cdm$cohort) %>%
 dplyr::filter(cohort_definition_id == 3) %>%
 visR::visr()

DBI::dbDisconnect(con, shutdown = TRUE)
}
```
