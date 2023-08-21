# CDMConnector 1.1.1

* add some bigquery support


# CDMConnector 1.1.0

* add `copy_cdm_to`, `copyCdmTo`
* add `generate_concept_cohort_set`, `generateConceptCohortSet`
* add more example CDM datasets
* add `record_cohort_attrition`, `recordCohortAttrition`
* improve database test coverage
* update vignettes

Released Aug 17, 2023

# CDMConnector 1.0.0

* remove `write_prefix` in `cdm_from_con` in favor of using the `write_schema` argument for prefixing
* remove `cdm_tables` argument from `cdm_from_con` in favor of `cdm_select_tbl` selection function
* add attributes to the cdm object to communicate downstream temp table preferences

Released June 7, 2023


# CDMConnector 0.6.0

* generate Capr cohorts with `generateCohortSet`
* add datepart function for extracting year, month, day parts of dates in dplyr
* fix datediff logic for years so it now returns number of complete years between two date
* add "write_prefix" attribute to cdm objects to support a namespace within the write_schema

Released May 5, 2023

# CDMConnector 0.5.1

* Add both camelCase and snake_case versions of all functions (@Tsemharb)
* Add cdm object attributes to `cdm_snapshot` output
* use `number_records` and `number_subjects` in cohort counts table
* Minor bug fixes

Released on March 20, 2023

# CDMConnector 0.5.0

* Define `generatedCohortSet` class
* Add `summarize_quantile` for cross database quantile queries
* Add `GeneratedCohortSet` object, constructor, and attribute accessor functions
* Add vignette on cohort generation
* Add `cdmSubset`, `cdmSubsetCohort`, `cdmSample` for subsetting a cdm
* Add `cdmFlatten` for transforming a cdm into a single flat table of observations
* Improve test coverage
* Make Java dependency optional
* `cdm_schema` is now required in `cdm_from_con` except for duckdb connections
* Remove visit_detail from default cdm tables
* Deprecate `computePermanent`
* Improve package website

Substantial contributions by @edward-burn, @catalamarti, @Tsemharb who are now
package co-authors.

Released Mar 8, 2023

# CDMConnector 0.4.1

* Add `computeQuery` function for creating temp and persistent tables from dplyr queries
* Download Eunomia from darwin-eu repository as zipped csv files
* Add passing tests for Oracle and Spark
* Add `asDate` wrapper that provides correct as.Date dbplyr translations
* Incorporate code review suggestions from @mvankessel-EMC

Released Jan 21, 2023

# CDMConnector 0.3.0

* Add computePermanent and appendPermanent functions
* Add readCohortSet
* Add generateCohortSet
* Extract Eunomia dataset so that it is downloaded separately

Released Nov 29, 2022

# CDMConnector 0.2.0

* Add support for OMOP v5.3 and v5.4
* Add datediff and dateadd functions that can be used in dplyr::mutate() 
* Add assert_tables function for checking that a cdm object has required tables
* Add camelCase versions of several functions

Released Nov 15, 2022

# CDMConnector 0.1.0

* Initial release Sept 11, 2022
* Added a `NEWS.md` file to track changes to the package.
