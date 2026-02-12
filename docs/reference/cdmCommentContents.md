# Insert Patient CDM Contents as Aligned Comments in RStudio

This function retrieves the longitudinal event table for one or more
persons in a CDM object and inserts it as a nicely formatted, R-style
comment block directly into your active RStudio document. This is
particularly useful for documenting reproducible test cases or examples
by showing relevant CDM contents inline in test scripts or analysis
code.

## Usage

``` r
cdmCommentContents(cdm, personIds = NULL)
```

## Arguments

- cdm:

  A CDMConnector cdm_reference object.

- personIds:

  Optional numeric vector of person IDs to filter the rows to include.
  If NULL (default), includes all persons in the cdm.

## Details

Each row of patient data will be aligned in columns as a commented
table, making it easy to copy, review, and maintain sample data
expectations in documentation or test suites.

Requires an interactive RStudio session with the `rstudioapi` package
available. The function utilizes
[`CDMConnector::cdmFlatten()`](cdmFlatten.md) to extract a longitudinal
view, and writes the commented results directly below the cursor in the
active RStudio document.

This workflow is especially helpful when documenting expected patient
timelines for use in testthat or other test scripts, or when sharing
reproducible CDM content for instructional examples.

## See also

[`cdmFlatten`](cdmFlatten.md)

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con, "main", "main")
cdmCommentContents(cdm, personIds = 6)
# person_id | observation_concept_id | start_date | end_date   | type_concept_id | domain               | observation_concept_name                                                                                                       | type_concept_name
# 6         | 40213296               | 2006-01-10 | 2006-01-10 | 581452          | drug_exposure        | hepatitis A vaccine, adult dosage                                                                                              | NA
# 6         | 40213227               | 2006-01-10 | 2006-01-10 | 581452          | drug_exposure        | tetanus and diphtheria toxoids, adsorbed, preservative free, for adult use                                                     | NA
# 6         | 1118084                | 2005-07-13 | 2005-07-13 | 38000177        | drug_exposure        | celecoxib                                                                                                                      | NA
# 6         | 80180                  | 2005-07-13 | NA         | 32020           | condition_occurrence | Osteoarthritis                                                                                                                 | NA
cdmDisconnect(cdm)
} # }
```
